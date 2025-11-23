import os
import threading
import time
import logging
from datetime import datetime, timezone, date, timedelta

import psycopg2
from psycopg2.extras import RealDictCursor
import psycopg2.extras
import re
import requests
from flask import Flask, jsonify, render_template, request

# ------------------ Config ------------------

API_URL_DEFAULT = "https://data-api.polymarket.com/trades"

DB_HOST = os.getenv("DB_HOST", "db")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "polymarket")
DB_USER = os.getenv("DB_USER", "polymarket")
DB_PASSWORD = os.getenv("DB_PASSWORD", "polymarket")

MIN_CASH_DEFAULT = float(os.getenv("MIN_CASH_USD", "10000"))
POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "10"))
LIMIT_DEFAULT = int(os.getenv("LIMIT", "1000"))

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s | %(levelname)-8s | %(message)s",
)

# Optional filters (not used by default)
MARKETS_FILTER = os.getenv("MARKET_CONDITION_IDS", "")
EVENT_IDS_FILTER = os.getenv("EVENT_IDS", "")
SIDE_FILTER = os.getenv("SIDE_FILTER", "").upper().strip()

# ------------------ DB helpers ------------------


def get_db_conn():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )


def init_db():
    conn = get_db_conn()
    try:
        with conn, conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS trades (
                    tx_hash      TEXT PRIMARY KEY,
                    wallet       TEXT,
                    pseudonym    TEXT,
                    side         TEXT,
                    market       TEXT,
                    outcome      TEXT,
                    event_slug   TEXT,
                    event_date   DATE,
                    category     TEXT,
                    size         NUMERIC,
                    price        NUMERIC,
                    notional     NUMERIC,
                    ts           TIMESTAMPTZ,
                    is_new_wallet BOOLEAN
                );
                """
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_trades_ts ON trades(ts DESC);"
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_trades_wallet ON trades(wallet);"
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_trades_category ON trades(category);"
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_trades_event_date ON trades(event_date);"
            )
        logging.info("DB initialized")
    finally:
        conn.close()


# ------------------ Classification helpers ------------------


def classify_category(market: str | None, event_slug: str | None) -> str:
    text = (market or "") + " " + (event_slug or "")
    t = text.lower()

    # crude but effective heuristics
    if any(
        key in t
        for key in [
            "nba-",
            "nfl-",
            "nhl-",
            "cfb-",
            "cbb-",
            "epl-",
            "lal-",
            "bun-",
            "elc-",
            "fl1-",
            "vs.",
            "dota2",
            "ufc",
            "premier league",
            "nhl-",
        ]
    ):
        return "sports"

    if any(
        key in t
        for key in [
            "bitcoin",
            "ethereum",
            "solana",
            "crypto",
            "token",
            "market cap",
            "fdv",
        ]
    ):
        return "crypto"

    if any(
        key in t
        for key in [
            "election",
            "president",
            "presidential",
            "parliament",
            "mayor",
            "house",
            "senate",
            "xi jinping",
            "trump",
            "biden",
            "maduro",
            "tariffs",
        ]
    ):
        return "politics"

    return "other"


def parse_event_date(event_slug: str | None) -> date | None:
    if not event_slug:
        return None
    parts = event_slug.split("-")
    if len(parts) < 3:
        return None
    candidate = "-".join(parts[-3:])
    try:
        return datetime.strptime(candidate, "%Y-%m-%d").date()
    except Exception:
        return None


def infer_category(event_slug: str, market: str) -> str:
    """Very dumb categorizer: sports / crypto / politics / other."""
    text = f"{event_slug} {market}".lower()

    if any(k in text for k in [
        "nba", "nfl", "nhl", "mlb", "cbb", "cfb",
        "vs.", " vs ", "spread:", "moneyline", "epl", "lal-", "cbb-"
    ]):
        return "sports"

    if any(k in text for k in [
        "bitcoin", "btc", "ethereum", "eth", "solana", "sol",
        "price of", "market cap", "fdv"
    ]):
        return "crypto"

    if any(k in text for k in [
        "election", "presidential", "senate", "house",
        "primary", "democratic", "republican",
        "trump", "biden", "xi jinping", "maduro", "putin"
    ]):
        return "politics"

    return "other"


# ------------------ Whale tracker ------------------


class WhaleTracker:
    def __init__(self):
        self.api_url = API_URL_DEFAULT
        self.min_cash_usd = MIN_CASH_DEFAULT
        self.limit = LIMIT_DEFAULT
        self.markets_filter = MARKETS_FILTER
        self.event_ids_filter = EVENT_IDS_FILTER
        self.side_filter = SIDE_FILTER

        self.last_seen_ts = 0
        self.seen_tx_hashes: set[str] = set()
        self.seen_wallets: set[str] = set()

    def fetch_trades(self):
        params = {
            "limit": self.limit,
            "takerOnly": "true",
            "filterType": "CASH",
            "filterAmount": self.min_cash_usd,
        }
        if self.markets_filter:
            params["market"] = self.markets_filter
        if self.event_ids_filter:
            params["eventId"] = self.event_ids_filter
        if self.side_filter in {"BUY", "SELL"}:
            params["side"] = self.side_filter

        resp = requests.get(self.api_url, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        if not isinstance(data, list):
            logging.warning("Unexpected API response type: %s", type(data))
            return []
        return data

    def process_trades(self, trades):
        if not trades:
            return

        # only new trades
        new_trades = []
        for t in trades:
            ts = int(t.get("timestamp", 0))
            tx = t.get("transactionHash") or ""
            if ts < self.last_seen_ts:
                continue
            if ts == self.last_seen_ts and tx in self.seen_tx_hashes:
                continue
            new_trades.append(t)

        new_trades.sort(key=lambda t: int(t.get("timestamp", 0)))
        if not new_trades:
            return

        conn = get_db_conn()
        try:
            with conn:
                with conn.cursor() as cur:
                    for t in new_trades:
                        self._insert_trade(cur, t)
        finally:
            conn.close()

        # update last_seen_ts and hashes
        for t in new_trades:
            ts = int(t.get("timestamp", 0))
            tx = t.get("transactionHash") or ""
            self.last_seen_ts = max(self.last_seen_ts, ts)
            if tx:
                self.seen_tx_hashes.add(tx)

    def _insert_trade(self, cur, t: dict):
        wallet = t.get("proxyWallet") or "unknown"
        pseudonym = t.get("pseudonym") or None
        side = t.get("side") or "?"
        market = t.get("title") or t.get("slug") or "unknown-market"
        outcome = t.get("outcome") or "?"
        event_slug = t.get("eventSlug") or None
        size = t.get("size")
        price = t.get("price")
        tx_hash = t.get("transactionHash") or None
        ts = int(t.get("timestamp", 0))

        # compute notional
        notional = None
        try:
            if isinstance(price, (int, float)) and isinstance(size, (int, float)):
                notional = float(price) * float(size)
        except Exception:
            pass

        # event date + category
        event_date = parse_event_date(event_slug)
        category = classify_category(market, event_slug)

        is_new_wallet = wallet not in self.seen_wallets
        self.seen_wallets.add(wallet)

        # log to console similar to before
        dt = datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
        logging.info(
            "[WHALE TRADE] [%s] Time(UTC): %s | Wallet: %s | Pseudonym: %s | Side: %s "
            "| Market: %s | Outcome: %s | Event slug: %s | Size: %s | Price: %s | "
            "Approx. notional: %s | Tx: %s",
            "NEW WALLET" if is_new_wallet else "EXISTING WALLET",
            dt,
            wallet,
            pseudonym or "n/a",
            side,
            market,
            outcome,
            event_slug or "",
            size,
            price,
            f"{notional:,.2f}" if notional is not None else "n/a",
            tx_hash,
        )

        # insert into DB
        if tx_hash is None:
            return  # skip weird ones

        cur.execute(
            """
            INSERT INTO trades (
                tx_hash, wallet, pseudonym, side, market, outcome,
                event_slug, event_date, category,
                size, price, notional, ts, is_new_wallet
            )
            VALUES (%s, %s, %s, %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s, to_timestamp(%s), %s)
            ON CONFLICT (tx_hash) DO NOTHING;
            """,
            (
                tx_hash,
                wallet,
                pseudonym,
                side,
                market,
                outcome,
                event_slug,
                event_date,
                category,
                size,
                price,
                notional,
                ts,
                is_new_wallet,
            ),
        )

    def run_forever(self):
        logging.info(
            "Starting whale ingestor | min_cash_usd=%.2f | interval=%ss | limit=%d",
            self.min_cash_usd,
            POLL_INTERVAL_SECONDS,
            self.limit,
        )
        if self.markets_filter:
            logging.info("Market filter: %s", self.markets_filter)
        if self.event_ids_filter:
            logging.info("Event IDs filter: %s", self.event_ids_filter)
        if self.side_filter:
            logging.info("Side filter: %s", self.side_filter)

        while True:
            try:
                trades = self.fetch_trades()
                self.process_trades(trades)
            except requests.RequestException as e:
                logging.error("API error: %s", e)
            except Exception as e:
                logging.exception("Unexpected error in ingestor: %s", e)
            time.sleep(POLL_INTERVAL_SECONDS)


# ------------------ Flask app ------------------

app = Flask(__name__)


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/summary")
def api_summary():
    """
    Returns:
      - top wallets (by notional in last N hours)
      - trade counts by category for last N hours
    """
    hours = int(request.args.get("hours", "24"))
    since = datetime.utcnow() - timedelta(hours=hours)

    conn = get_db_conn()
    try:
        with conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            # top wallets
            cur.execute(
                """
                SELECT wallet,
                       COALESCE(pseudonym, '') AS pseudonym,
                       COUNT(*) AS trades,
                       COALESCE(SUM(notional), 0) AS total_notional
                FROM trades
                WHERE ts >= %s
                GROUP BY wallet, pseudonym
                ORDER BY total_notional DESC
                LIMIT 20;
                """,
                (since,),
            )
            top_wallets = cur.fetchall()

            # category counts
            cur.execute(
                """
                SELECT category, COUNT(*) AS count
                FROM trades
                WHERE ts >= %s
                GROUP BY category;
                """,
                (since,),
            )
            rows = cur.fetchall()
    finally:
        conn.close()

    category_counts = {r["category"]: int(r["count"]) for r in rows}

    return jsonify(
        {
            "hours": hours,
            "top_wallets": top_wallets,
            "category_counts": category_counts,
        }
    )


@app.route("/api/trades")
def api_trades():
    """
    Filters:
      category=all|sports|crypto|politics|other
      min_notional=float
      new_only=true|false
      days_ahead=int (0=today only, 1=today+tomorrow, etc)
      limit=int
    """
    category = request.args.get("category", "all").lower()
    min_notional = float(request.args.get("min_notional", "10000"))
    new_only = request.args.get("new_only", "false").lower() == "true"
    days_ahead = request.args.get("days_ahead")
    limit = int(request.args.get("limit", "200"))

    where_clauses = ["notional IS NOT NULL", "notional >= %s"]
    params = [min_notional]

    if category in {"sports", "crypto", "politics", "other"}:
        where_clauses.append("category = %s")
        params.append(category)

    if new_only:
        where_clauses.append("is_new_wallet = TRUE")

    if days_ahead is not None:
        try:
            d = int(days_ahead)
            today = date.today()
            end_date = today + timedelta(days=d)
            where_clauses.append("event_date IS NOT NULL")
            where_clauses.append("event_date BETWEEN %s AND %s")
            params.extend([today, end_date])
        except ValueError:
            pass

    where_sql = " AND ".join(where_clauses)

    conn = get_db_conn()
    try:
        with conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                f"""
                SELECT
                    wallet,
                    COALESCE(pseudonym, '') AS pseudonym,
                    side,
                    market,
                    outcome,
                    event_slug,
                    event_date,
                    category,
                    size,
                    price,
                    notional,
                    ts,
                    is_new_wallet
                FROM trades
                WHERE {where_sql}
                ORDER BY ts DESC
                LIMIT %s;
                """,
                (*params, limit),
            )
            rows = cur.fetchall()
    finally:
        conn.close()

    # jsonify with isoformat timestamps
    trades = []
    for r in rows:
        trades.append(
            {
                "wallet": r["wallet"],
                "pseudonym": r["pseudonym"],
                "side": r["side"],
                "market": r["market"],
                "outcome": r["outcome"],
                "event_slug": r["event_slug"],
                "event_date": r["event_date"].isoformat()
                if r["event_date"]
                else None,
                "category": r["category"],
                "size": float(r["size"]) if r["size"] is not None else None,
                "price": float(r["price"]) if r["price"] is not None else None,
                "notional": float(r["notional"])
                if r["notional"] is not None
                else None,
                "ts": r["ts"].isoformat() if r["ts"] else None,
                "is_new_wallet": r["is_new_wallet"],
            }
        )

    return jsonify({"trades": trades})


def start_ingestor_thread():
    tracker = WhaleTracker()
    t = threading.Thread(target=tracker.run_forever, daemon=True)
    t.start()
    logging.info("Whale ingestor background thread started")


@app.route("/api/top_wallets")
def api_top_wallets():
    """
    Aggregate top wallets over the last N hours by notional.
    - ?hours=24
    - ?limit=20
    """
    try:
        hours = max(1, min(int(request.args.get("hours", 24)), 72))
    except ValueError:
        hours = 24

    try:
        limit = min(int(request.args.get("limit", 20)), 100)
    except ValueError:
        limit = 20

    conn = get_db_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cur.execute(
        """
        SELECT
            wallet,
            COALESCE(pseudonym, 'n/a') AS pseudonym,
            COUNT(*) AS trade_count,
            SUM(notional) AS total_notional
        FROM trades
        WHERE ts >= (NOW() AT TIME ZONE 'utc' - (%s || ' hours')::interval)
        GROUP BY wallet, pseudonym
        ORDER BY total_notional DESC
        LIMIT %s
        """,
        (hours, limit),
    )

    rows = cur.fetchall()
    conn.close()

    out = []
    for r in rows:
        out.append({
            "wallet": r["wallet"],
            "pseudonym": r["pseudonym"],
            "trade_count": int(r["trade_count"]),
            "total_notional": float(r["total_notional"] or 0.0),
        })

    return jsonify({"wallets": out})


if __name__ == "__main__":
    init_db()
    start_ingestor_thread()
    # Simple built-in server is fine for this use case
    app.run(host="0.0.0.0", port=8000)


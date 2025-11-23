const categorySelect = document.getElementById("categorySelect");
const minNotionalInput = document.getElementById("minNotionalInput");
const daysAheadInput = document.getElementById("daysAheadInput");
const newOnlyInput = document.getElementById("newOnlyInput");
const refreshButton = document.getElementById("refreshButton");

const topWalletsBody = document.getElementById("topWalletsBody");
const categoryCountsDiv = document.getElementById("categoryCounts");
const tradesBody = document.getElementById("tradesBody");

// treat trades > this as "big spikes" for styling
const BIG_NOTIONAL_THRESHOLD = 50000;

async function fetchJSON(url) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return res.json();
}

function shortenWallet(w) {
  if (!w || w.length < 10) return w;
  return `${w.slice(0, 6)}…${w.slice(-4)}`;
}

function formatNumber(x) {
  if (x === null || x === undefined || isNaN(x)) return "";
  return x.toLocaleString(undefined, {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  });
}

async function loadSummary() {
  try {
    const data = await fetchJSON("/api/summary?hours=24");
    // category counts
    const cc = data.category_counts || {};
    const parts = [];
    for (const [cat, count] of Object.entries(cc)) {
      parts.push(`${cat}: ${count}`);
    }
    categoryCountsDiv.textContent = parts.join(" | ");

    // top wallets
    topWalletsBody.innerHTML = "";
    (data.top_wallets || []).forEach((w) => {
      const tr = document.createElement("tr");
      tr.innerHTML = `
        <td>${shortenWallet(w.wallet)}</td>
        <td>${w.pseudonym || ""}</td>
        <td>${w.trades}</td>
        <td>${formatNumber(Number(w.total_notional || 0))}</td>
      `;
      topWalletsBody.appendChild(tr);
    });
  } catch (e) {
    console.error("Summary error", e);
  }
}

async function loadTrades() {
  const category = categorySelect.value;
  const minNotional = Number(minNotionalInput.value || 0);
  const daysAhead = Number(daysAheadInput.value || 0);
  const newOnly = newOnlyInput.checked;

  const params = new URLSearchParams();
  params.set("min_notional", String(minNotional));
  params.set("limit", "200");
  if (category !== "all") params.set("category", category);
  if (daysAhead >= 0) params.set("days_ahead", String(daysAhead));
  if (newOnly) params.set("new_only", "true");

  try {
    const data = await fetchJSON(`/api/trades?${params.toString()}`);
    tradesBody.innerHTML = "";

    (data.trades || []).forEach((t) => {
      const tr = document.createElement("tr");
      if (t.is_new_wallet) {
        tr.classList.add("new-wallet");
      }

      const tdNotionalClass =
        t.notional && t.notional >= BIG_NOTIONAL_THRESHOLD
          ? "notional-big"
          : "";

      tr.innerHTML = `
        <td>${t.ts || ""}</td>
        <td>${t.category || ""}</td>
        <td>${shortenWallet(t.wallet)}</td>
        <td>${t.pseudonym || ""}</td>
        <td>${t.side || ""}</td>
        <td>${t.market || ""}</td>
        <td>${t.outcome || ""}</td>
        <td>${t.event_date || ""}</td>
        <td class="${tdNotionalClass}">${t.notional ? formatNumber(t.notional) : ""}</td>
        <td>${t.is_new_wallet ? "NEW" : ""}</td>
      `;
      tradesBody.appendChild(tr);
    });
  } catch (e) {
    console.error("Trades error", e);
  }
}

function refreshAll() {
  loadSummary();
  loadTrades();
}

refreshButton.addEventListener("click", () => {
  refreshAll();
});

// auto-refresh every 5 seconds
setInterval(refreshAll, 5000);

// initial load
refreshAll();


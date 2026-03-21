const state = {
  selectedCode: "",
};

async function apiGet(path, params = {}) {
  const query = new URLSearchParams();
  Object.entries(params).forEach(([key, value]) => {
    if (value == null) return;
    const text = String(value).trim();
    if (!text) return;
    query.set(key, text);
  });
  const url = query.toString() ? `${path}?${query.toString()}` : path;
  const response = await fetch(url);
  if (!response.ok) {
    throw new Error(`${response.status} ${response.statusText}`);
  }
  return response.json();
}

function formatNumber(value) {
  if (value == null || value === "") return "-";
  const number = Number(value);
  return Number.isFinite(number) ? number.toLocaleString("ko-KR") : String(value);
}

function setRows(targetId, rowsHtml, colspan, emptyText) {
  const target = document.getElementById(targetId);
  if (!target) return;
  target.innerHTML = rowsHtml || `<tr><td colspan="${colspan}">${emptyText}</td></tr>`;
}

function renderSummary(data) {
  const target = document.getElementById("summary");
  if (!target) return;
  const items = [
    ["?? ?", data.instrument_count],
    ["?? ??", data.price_count],
    ["?? ??", data.price_date_from && data.price_date_to ? `${data.price_date_from} ~ ${data.price_date_to}` : "-"],
    ["???? ??", data.benchmark_count],
    ["???? ??", data.benchmark_date_from && data.benchmark_date_to ? `${data.benchmark_date_from} ~ ${data.benchmark_date_to}` : "-"],
  ];
  target.innerHTML = items.map(([label, value]) => `
    <div class="card">
      <div class="label">${label}</div>
      <div class="value">${formatNumber(value)}</div>
    </div>
  `).join("");
}

function renderInstruments(items) {
  const rowsHtml = items.map((item) => `
    <tr data-code="${item.external_code}">
      <td><button class="link-button" type="button">${item.external_code}</button></td>
      <td>${item.instrument_name}</td>
      <td>${item.market_code}</td>
      <td>${item.listed_status === "delisted" ? "??" : "??"}</td>
      <td>${item.listing_date || "-"}</td>
      <td>${item.delisting_date || "-"}</td>
    </tr>
  `).join("");
  setRows("instrumentRows", rowsHtml, 6, "?? ??? ????.");
  document.querySelectorAll("#instrumentRows tr[data-code]").forEach((row) => {
    row.addEventListener("click", () => loadInstrument(row.dataset.code || ""));
  });
}

function renderProfile(profile) {
  const target = document.getElementById("instrumentProfile");
  if (!target) return;
  if (!profile || !profile.external_code) {
    target.innerHTML = '<div class="card"><div class="label">??? ??</div><div class="value">-</div></div>';
    return;
  }
  const items = [
    ["??", profile.external_code],
    ["???", profile.instrument_name],
    ["??", profile.market_code],
    ["??", profile.listed_status === "delisted" ? "??" : "??"],
    ["???", profile.listing_date],
    ["???", profile.delisting_date],
    ["?????", profile.listed_shares],
    ["????", profile.delisting_reason],
  ];
  target.innerHTML = items.map(([label, value]) => `
    <div class="card compact">
      <div class="label">${label}</div>
      <div class="value small">${formatNumber(value)}</div>
    </div>
  `).join("");
}

function renderPrices(items) {
  const rowsHtml = items.map((item) => `
    <tr>
      <td>${item.trade_date}</td>
      <td>${formatNumber(item.close)}</td>
      <td>${formatNumber(item.adj_close)}</td>
      <td>${formatNumber(item.volume)}</td>
      <td>${formatNumber(item.turnover_value)}</td>
      <td>${formatNumber(item.market_value)}</td>
      <td>${formatNumber(item.base_price)}</td>
      <td>${formatNumber(item.cumulative_factor)}</td>
    </tr>
  `).join("");
  setRows("priceRows", rowsHtml, 8, "?? ??? ????.");
}

function renderBenchmarks(items) {
  const rowsHtml = items.map((item) => `
    <tr>
      <td>${item.trade_date}</td>
      <td>${item.index_name}</td>
      <td>${formatNumber(item.close)}</td>
      <td>${formatNumber(item.volume)}</td>
      <td>${formatNumber(item.turnover_value)}</td>
      <td>${formatNumber(item.market_cap)}</td>
    </tr>
  `).join("");
  setRows("benchmarkRows", rowsHtml, 6, "?? ??? ????.");
}

async function loadSummary() {
  const data = await apiGet("/api/v1/dashboard/summary");
  renderSummary(data);
}

async function loadInstruments() {
  setRows("instrumentRows", "", 6, "?? ?...");
  const search = document.getElementById("searchInput")?.value || "";
  const listedStatus = document.getElementById("statusSelect")?.value || "";
  const data = await apiGet("/api/v1/instruments", { search, listed_status: listedStatus, limit: 50, offset: 0 });
  renderInstruments(data.items || []);
}

async function loadInstrument(externalCode) {
  if (!externalCode) return;
  state.selectedCode = externalCode;
  const profile = await apiGet(`/api/v1/instruments/${encodeURIComponent(externalCode)}`);
  renderProfile(profile);
  await loadPrices();
}

async function loadPrices() {
  if (!state.selectedCode) {
    setRows("priceRows", "", 8, "??? ?????.");
    return;
  }
  setRows("priceRows", "", 8, "?? ?...");
  const dateFrom = document.getElementById("fromInput")?.value || "";
  const dateTo = document.getElementById("toInput")?.value || "";
  const data = await apiGet(`/api/v1/instruments/${encodeURIComponent(state.selectedCode)}/daily`, {
    date_from: dateFrom,
    date_to: dateTo,
    limit: 250,
    offset: 0,
  });
  renderPrices(data.items || []);
}

async function loadBenchmarks() {
  setRows("benchmarkRows", "", 6, "?? ?...");
  const indexCode = document.getElementById("benchmarkCode")?.value || "KOSPI";
  const seriesName = document.getElementById("benchmarkSeries")?.value || "";
  const dateFrom = document.getElementById("fromInput")?.value || "";
  const dateTo = document.getElementById("toInput")?.value || "";
  const data = await apiGet(`/api/v1/benchmarks/${encodeURIComponent(indexCode)}/daily`, {
    series_name: seriesName,
    date_from: dateFrom,
    date_to: dateTo,
    limit: 250,
    offset: 0,
  });
  renderBenchmarks(data.items || []);
}

function bindEvents() {
  document.getElementById("searchButton")?.addEventListener("click", () => loadInstruments().catch(console.error));
  document.getElementById("priceButton")?.addEventListener("click", () => loadPrices().catch(console.error));
  document.getElementById("benchmarkButton")?.addEventListener("click", () => loadBenchmarks().catch(console.error));
}

async function init() {
  bindEvents();
  renderProfile(null);
  await loadSummary();
  await loadInstruments();
  await loadBenchmarks();
}

window.addEventListener("DOMContentLoaded", () => {
  init().catch((error) => {
    console.error(error);
    setRows("instrumentRows", "", 6, "???? ???? ?????.");
    setRows("priceRows", "", 8, "???? ???? ?????.");
    setRows("benchmarkRows", "", 6, "???? ???? ?????.");
  });
});

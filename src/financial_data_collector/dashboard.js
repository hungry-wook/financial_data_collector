const MA_SETTINGS_KEY = "dashboard_ma_settings_v1";
const DEFAULT_PRICE_MA = [5, 20];
const DEFAULT_VOLUME_MA = [5, 20];
const MIN_MA_PERIOD = 2;
const MAX_MA_PERIOD = 240;
const MAX_MA_COUNT = 8;
const MA_COLORS = ["#f59e0b", "#ef4444", "#8b5cf6", "#0ea5e9", "#10b981", "#64748b", "#f97316", "#d946ef"];
const BULL_COLOR = "#dc2626";
const BEAR_COLOR = "#2563eb";
const BULL_VOLUME_COLOR = "rgba(220,38,38,0.45)";
const BEAR_VOLUME_COLOR = "rgba(37,99,235,0.45)";
const PRICE_SCALE_MIN_WIDTH = 80;

const dashboardApi = {
  async get(path, params = {}) {
    const q = new URLSearchParams();
    Object.entries(params).forEach(([key, value]) => {
      if (value == null) return;
      const text = String(value).trim();
      if (!text) return;
      q.set(key, text);
    });
    const url = q.toString() ? `${path}?${q.toString()}` : path;
    return fetch(url).then((r) => r.json());
  },
};

function toNumber(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function parseBusinessDay(value) {
  const text = String(value || "");
  const parts = text.split("-");
  if (parts.length !== 3) return null;
  const year = Number(parts[0]);
  const month = Number(parts[1]);
  const day = Number(parts[2]);
  if (!Number.isInteger(year) || !Number.isInteger(month) || !Number.isInteger(day)) return null;
  return { year, month, day };
}

function formatCompactBusinessDay(time) {
  if (!time || typeof time !== "object") return "";
  const year = Number(time.year);
  const month = Number(time.month);
  const day = Number(time.day);
  if (!Number.isInteger(year) || !Number.isInteger(month) || !Number.isInteger(day)) return "";
  return `${String(year).padStart(4, "0")}${String(month).padStart(2, "0")}${String(day).padStart(2, "0")}`;
}

function parseCompactBusinessDay(value) {
  const text = String(value || "").trim();
  if (!/^\d{8}$/.test(text)) return null;
  const year = Number(text.slice(0, 4));
  const month = Number(text.slice(4, 6));
  const day = Number(text.slice(6, 8));
  if (!Number.isInteger(year) || !Number.isInteger(month) || !Number.isInteger(day)) return null;
  return { year, month, day };
}

function isChartPointInside(param) {
  const x = param?.point?.x;
  const y = param?.point?.y;
  return Number.isFinite(x) && Number.isFinite(y) && x >= 0 && y >= 0;
}

function normalizePeriods(values) {
  const unique = new Set();
  (Array.isArray(values) ? values : []).forEach((value) => {
    const n = Number(value);
    if (!Number.isInteger(n)) return;
    if (n < MIN_MA_PERIOD || n > MAX_MA_PERIOD) return;
    unique.add(n);
  });
  return Array.from(unique).sort((a, b) => a - b).slice(0, MAX_MA_COUNT);
}

function computeSMA(rows, period, selector) {
  const out = [];
  if (!Array.isArray(rows) || rows.length < period) return out;
  for (let i = period - 1; i < rows.length; i += 1) {
    let sum = 0;
    let valid = true;
    for (let j = i - period + 1; j <= i; j += 1) {
      const value = selector(rows[j]);
      if (!Number.isFinite(value)) {
        valid = false;
        break;
      }
      sum += value;
    }
    if (!valid) continue;
    out.push({ time: rows[i].time, value: sum / period });
  }
  return out;
}

function getMAColor(idx) {
  return MA_COLORS[idx % MA_COLORS.length];
}

function toVolumeCandlePoint(row, upColor) {
  const value = Number.isFinite(row?.volume) ? row.volume : 0;
  const isUp = row?.close >= row?.open;
  const color = isUp ? upColor : BEAR_VOLUME_COLOR;
  return {
    time: row.time,
    open: 0,
    high: value,
    low: 0,
    close: value,
    color,
    borderColor: color,
    wickColor: color,
  };
}

function toVolumeHistogramPoint(row, upColor) {
  const value = Number.isFinite(row?.volume) ? row.volume : 0;
  const isUp = row?.close >= row?.open;
  return {
    time: row.time,
    value,
    color: isUp ? upColor : BEAR_VOLUME_COLOR,
  };
}

function dashboard() {
  return {
    tabs: [
      { id: "overview", label: "개요" },
      { id: "instruments", label: "종목·시세" },
      { id: "quality", label: "품질" },
    ],
    activeTab: "overview",
    lastRefresh: "-",

    summary: {},
    runs: [],
    runsLoading: false,

    instrumentQuery: "",
    instrumentOptions: [],
    instrumentTotal: null,
    instrumentLimit: 20,
    instrumentOffset: 0,
    instrumentHasMore: false,
    selectedInstrument: null,
    instrumentProfile: null,
    optionLoading: false,
    _instrumentSearchTimer: null,

    prices: [],
    priceTotal: null,
    priceLimit: 300,
    priceOffset: 0,
    priceHasMore: false,
    priceLoading: false,
    priceQueried: false,
    _priceChartCtx: null,
    _priceRequestSeq: 0,
    priceChartError: "",
    showInstrumentBench: false,
    instrumentBenchSeries: [],
    instrumentBenchLoading: false,
    instrumentBenchQueried: false,
    instrumentBenchError: "",
    instrumentBenchIndexCode: "",
    instrumentBenchSeriesName: "",
    instrumentBenchSeriesOptions: [],
    instrumentBenchTotal: null,
    instrumentBenchLimit: 300,
    instrumentBenchOffset: 0,
    instrumentBenchHasMore: false,
    _instrumentBenchChartCtx: null,
    _instrumentBenchRequestSeq: 0,
    _instrumentChartSyncGuard: false,
    _instrumentTimeSyncUnsubs: [],
    _instrumentCrosshairSyncUnsubs: [],

    benchmarks: [],
    benchSeriesOptions: [],
    benchSeries: [],
    selectedIndex: "",
    selectedSeries: "",
    benchFrom: "",
    benchTo: "",
    benchLoading: false,
    benchTotal: 0,
    benchLimit: 1000,
    benchOffset: 0,
    _benchChartCtx: null,
    benchChartError: "",

    priceMAPeriods: DEFAULT_PRICE_MA.slice(),
    volumeMAPeriods: DEFAULT_VOLUME_MA.slice(),
    priceMAInput: "",
    volumeMAInput: "",
    maSettingsError: "",

    qualityIssues: [],
    qualitySeverity: "",
    qualityLoading: false,

    async init() {
      this.loadMASettings();
      await Promise.all([this.loadSummary(), this.loadRuns(), this.loadBenchmarks()]);
      this.lastRefresh = new Date().toLocaleTimeString("ko-KR");
      window.addEventListener("resize", () => this.resizeCharts());
    },

    switchTab(id) {
      this.activeTab = id;
      if (id === "instruments" && this.instrumentOptions.length === 0) {
        this.searchInstrumentOptions();
      }
      if (id === "quality" && this.qualityIssues.length === 0) {
        this.loadQualityIssues();
      }
      this.$nextTick(() => this.resizeCharts());
    },

    loadMASettings() {
      try {
        const raw = localStorage.getItem(MA_SETTINGS_KEY);
        if (!raw) {
          this.priceMAPeriods = DEFAULT_PRICE_MA.slice();
          this.volumeMAPeriods = DEFAULT_VOLUME_MA.slice();
          return;
        }
        const parsed = JSON.parse(raw);
        this.priceMAPeriods = normalizePeriods(parsed?.price?.length ? parsed.price : DEFAULT_PRICE_MA);
        this.volumeMAPeriods = normalizePeriods(parsed?.volume?.length ? parsed.volume : DEFAULT_VOLUME_MA);
        if (this.priceMAPeriods.length === 0) this.priceMAPeriods = DEFAULT_PRICE_MA.slice();
        if (this.volumeMAPeriods.length === 0) this.volumeMAPeriods = DEFAULT_VOLUME_MA.slice();
      } catch (_) {
        this.priceMAPeriods = DEFAULT_PRICE_MA.slice();
        this.volumeMAPeriods = DEFAULT_VOLUME_MA.slice();
      }
    },

    saveMASettings() {
      localStorage.setItem(
        MA_SETTINGS_KEY,
        JSON.stringify({
          price: normalizePeriods(this.priceMAPeriods),
          volume: normalizePeriods(this.volumeMAPeriods),
        })
      );
    },

    _addMAPeriod(kind, value) {
      const period = Number(value);
      if (!Number.isInteger(period)) {
        this.maSettingsError = "숫자 기간을 입력해 주세요.";
        return false;
      }
      if (period < MIN_MA_PERIOD || period > MAX_MA_PERIOD) {
        this.maSettingsError = `기간은 ${MIN_MA_PERIOD}~${MAX_MA_PERIOD} 사이여야 합니다.`;
        return false;
      }
      const target = kind === "price" ? this.priceMAPeriods : this.volumeMAPeriods;
      if (target.includes(period)) {
        this.maSettingsError = "이미 추가된 기간입니다.";
        return false;
      }
      if (target.length >= MAX_MA_COUNT) {
        this.maSettingsError = `최대 ${MAX_MA_COUNT}개까지 추가할 수 있습니다.`;
        return false;
      }
      target.push(period);
      target.sort((a, b) => a - b);
      this.maSettingsError = "";
      this.saveMASettings();
      this.refreshAllCharts();
      return true;
    },

    _removeMAPeriod(kind, period) {
      const target = kind === "price" ? this.priceMAPeriods : this.volumeMAPeriods;
      const next = target.filter((p) => p !== period);
      if (next.length === target.length) return;
      if (kind === "price") this.priceMAPeriods = next;
      else this.volumeMAPeriods = next;
      this.maSettingsError = "";
      this.saveMASettings();
      this.refreshAllCharts();
    },

    addPriceMA(period) {
      this._addMAPeriod("price", period);
    },

    addPriceMAInput() {
      if (this._addMAPeriod("price", this.priceMAInput)) this.priceMAInput = "";
    },

    removePriceMA(period) {
      this._removeMAPeriod("price", period);
    },

    addVolumeMA(period) {
      this._addMAPeriod("volume", period);
    },

    addVolumeMAInput() {
      if (this._addMAPeriod("volume", this.volumeMAInput)) this.volumeMAInput = "";
    },

    removeVolumeMA(period) {
      this._removeMAPeriod("volume", period);
    },

    refreshAllCharts() {
      if (this.prices.length > 0) this.renderPriceChart();
      if (this.benchSeries.length > 0) this.renderBenchChart();
    },

    _clearInstrumentChartSync() {
      this._instrumentTimeSyncUnsubs.forEach((unsub) => {
        try {
          unsub();
        } catch (_) {}
      });
      this._instrumentCrosshairSyncUnsubs.forEach((unsub) => {
        try {
          unsub();
        } catch (_) {}
      });
      this._instrumentTimeSyncUnsubs = [];
      this._instrumentCrosshairSyncUnsubs = [];
      this._instrumentChartSyncGuard = false;
    },

    _wireInstrumentTimeSync() {
      const src = this._priceChartCtx?.chart;
      const dst = this._instrumentBenchChartCtx?.chart;
      if (!src || !dst) return;
      if (!src.timeScale || !dst.timeScale) return;
      const srcTs = src.timeScale();
      const dstTs = dst.timeScale();
      if (!srcTs || !dstTs) return;

      const canSyncByTimeRange =
        typeof srcTs.subscribeVisibleTimeRangeChange === "function" &&
        typeof dstTs.subscribeVisibleTimeRangeChange === "function" &&
        typeof srcTs.setVisibleRange === "function" &&
        typeof dstTs.setVisibleRange === "function";
      const canSyncByLogicalRange =
        typeof srcTs.subscribeVisibleLogicalRangeChange === "function" &&
        typeof dstTs.subscribeVisibleLogicalRangeChange === "function" &&
        typeof srcTs.setVisibleLogicalRange === "function" &&
        typeof dstTs.setVisibleLogicalRange === "function";
      if (!canSyncByTimeRange && !canSyncByLogicalRange) return;

      const bindOneWay = (fromTs, toTs, mode) => {
        const handler = (range) => {
          if (!range) return;
          if (this._instrumentChartSyncGuard) return;
          this._instrumentChartSyncGuard = true;
          try {
            if (mode === "time") toTs.setVisibleRange(range);
            else toTs.setVisibleLogicalRange(range);
          } catch (_) {
          } finally {
            this._instrumentChartSyncGuard = false;
          }
        };
        if (mode === "time") fromTs.subscribeVisibleTimeRangeChange(handler);
        else fromTs.subscribeVisibleLogicalRangeChange(handler);
        return () => {
          if (mode === "time") fromTs.unsubscribeVisibleTimeRangeChange(handler);
          else fromTs.unsubscribeVisibleLogicalRangeChange(handler);
        };
      };

      if (canSyncByTimeRange) {
        this._instrumentTimeSyncUnsubs.push(bindOneWay(srcTs, dstTs, "time"));
        this._instrumentTimeSyncUnsubs.push(bindOneWay(dstTs, srcTs, "time"));
        return;
      }
      this._instrumentTimeSyncUnsubs.push(bindOneWay(srcTs, dstTs, "logical"));
      this._instrumentTimeSyncUnsubs.push(bindOneWay(dstTs, srcTs, "logical"));
    },

    _syncInstrumentBenchRangeFromPrice() {
      const src = this._priceChartCtx?.chart;
      const dst = this._instrumentBenchChartCtx?.chart;
      if (!src || !dst || !src.timeScale || !dst.timeScale) return;
      const srcTs = src.timeScale();
      const dstTs = dst.timeScale();
      if (!srcTs || !dstTs) return;

      if (typeof srcTs.getVisibleRange === "function" && typeof dstTs.setVisibleRange === "function") {
        const visibleRange = srcTs.getVisibleRange();
        if (visibleRange) {
          try {
            dstTs.setVisibleRange(visibleRange);
            return;
          } catch (_) {}
        }
      }

      if (typeof srcTs.getVisibleLogicalRange !== "function" || typeof dstTs.setVisibleLogicalRange !== "function") return;
      const logicalRange = srcTs.getVisibleLogicalRange();
      if (!logicalRange) return;
      try {
        dstTs.setVisibleLogicalRange(logicalRange);
      } catch (_) {}
    },

    _alignInstrumentChartsTimeline() {
      const priceCtx = this._priceChartCtx;
      const benchCtx = this._instrumentBenchChartCtx;
      if (!priceCtx?.candles || !priceCtx?.volume || !benchCtx?.candles || !benchCtx?.volume) return;

      const priceRows = Array.isArray(priceCtx.baseRows) ? priceCtx.baseRows : [];
      const benchRows = Array.isArray(benchCtx.baseRows) ? benchCtx.baseRows : [];
      if (priceRows.length === 0 || benchRows.length === 0) return;

      const keys = Array.from(
        new Set(
          priceRows
            .concat(benchRows)
            .map((row) => formatCompactBusinessDay(row?.time))
            .filter(Boolean)
        )
      ).sort();
      if (keys.length === 0) return;

      const alignRows = (rows) => {
        const map = new Map(rows.map((row) => [formatCompactBusinessDay(row.time), row]));
        return keys
          .map((key) => {
            const row = map.get(key);
            if (row) return row;
            const time = parseCompactBusinessDay(key);
            return time ? { time } : null;
          })
          .filter(Boolean);
      };

      const toCandleData = (rows) => rows.map((r) => (r.open == null ? { time: r.time } : { time: r.time, open: r.open, high: r.high, low: r.low, close: r.close }));
      const toVolumeData = (rows) => rows.map((r) => (r.open == null ? { time: r.time } : toVolumeCandlePoint(r, BULL_VOLUME_COLOR)));

      const alignedPriceRows = alignRows(priceRows);
      const alignedBenchRows = alignRows(benchRows);

      priceCtx.candles.setData(toCandleData(alignedPriceRows));
      priceCtx.volume.setData(toVolumeData(alignedPriceRows));
      benchCtx.candles.setData(toCandleData(alignedBenchRows));
      benchCtx.volume.setData(toVolumeData(alignedBenchRows));

      priceCtx.timeKeyMap = new Map(priceRows.map((row) => [formatCompactBusinessDay(row.time), row]));
      benchCtx.timeKeyMap = new Map(benchRows.map((row) => [formatCompactBusinessDay(row.time), row]));
    },

    _wireInstrumentCrosshairSync() {
      const sourceCtx = this._priceChartCtx;
      const targetCtx = this._instrumentBenchChartCtx;
      if (!sourceCtx?.chart || !targetCtx?.chart) return;
      if (typeof sourceCtx.chart.subscribeCrosshairMove !== "function") return;
      if (typeof targetCtx.chart.subscribeCrosshairMove !== "function") return;

      const bindOneWay = (fromCtx, toCtx) => {
        const moveFn = toCtx.chart.setCrosshairPosition;
        const clearFn = toCtx.chart.clearCrosshairPosition;
        if (typeof moveFn !== "function") return () => {};

        const handler = (param) => {
          if (this._instrumentChartSyncGuard) return;
          this._instrumentChartSyncGuard = true;
          try {
            if (!param || !param.time || !isChartPointInside(param)) {
              if (typeof clearFn === "function") clearFn.call(toCtx.chart);
              return;
            }
            const key = formatCompactBusinessDay(param.time);
            const row = toCtx.timeKeyMap?.get(key);
            if (!row) {
              if (typeof clearFn === "function") clearFn.call(toCtx.chart);
              return;
            }
            moveFn.call(toCtx.chart, row.close, param.time, toCtx.candles);
          } catch (_) {
          } finally {
            this._instrumentChartSyncGuard = false;
          }
        };

        fromCtx.chart.subscribeCrosshairMove(handler);
        return () => fromCtx.chart.unsubscribeCrosshairMove(handler);
      };

      this._instrumentCrosshairSyncUnsubs.push(bindOneWay(sourceCtx, targetCtx));
      this._instrumentCrosshairSyncUnsubs.push(bindOneWay(targetCtx, sourceCtx));
    },

    _syncInstrumentChartScaleWidth() {
      const priceChart = this._priceChartCtx?.chart;
      const benchChart = this._instrumentBenchChartCtx?.chart;
      if (!priceChart || !benchChart) return;

      const readWidth = (chart) => {
        try {
          const scale = chart.priceScale("right");
          const width = typeof scale?.width === "function" ? Number(scale.width()) : NaN;
          return Number.isFinite(width) && width > 0 ? width : PRICE_SCALE_MIN_WIDTH;
        } catch (_) {
          return PRICE_SCALE_MIN_WIDTH;
        }
      };

      const syncedWidth = Math.max(readWidth(priceChart), readWidth(benchChart), PRICE_SCALE_MIN_WIDTH);
      priceChart.applyOptions({ rightPriceScale: { minimumWidth: syncedWidth } });
      benchChart.applyOptions({ rightPriceScale: { minimumWidth: syncedWidth } });
    },

    _rewireInstrumentChartSyncIfReady() {
      this._clearInstrumentChartSync();
      if (!this.showInstrumentBench) return;
      if (!this._priceChartCtx || !this._instrumentBenchChartCtx) return;
      this._alignInstrumentChartsTimeline();
      this._syncInstrumentChartScaleWidth();
      this._wireInstrumentTimeSync();
      this._wireInstrumentCrosshairSync();
      this._syncInstrumentBenchRangeFromPrice();
    },

    _createChartOptions(container) {
      return {
        width: Math.max(container.clientWidth || 0, 320),
        height: Math.max(container.clientHeight || 0, 320),
        layout: {
          background: { color: "#ffffff" },
          textColor: "#334155",
        },
        grid: {
          vertLines: { color: "#eef2f7" },
          horzLines: { color: "#eef2f7" },
        },
        timeScale: {
          borderColor: "#cbd5e1",
          timeVisible: false,
          secondsVisible: false,
          tickMarkFormatter: (time) => formatCompactBusinessDay(time),
        },
        rightPriceScale: {
          borderColor: "#cbd5e1",
          scaleMargins: { top: 0.08, bottom: 0.34 },
          minimumWidth: PRICE_SCALE_MIN_WIDTH,
        },
        localization: {
          locale: "ko-KR",
          timeFormatter: (time) => formatCompactBusinessDay(time),
        },
      };
    },

    _createChartContext(containerId) {
      const container = document.getElementById(containerId);
      if (!(container instanceof HTMLDivElement) || !window.LightweightCharts) return null;
      const { createChart } = window.LightweightCharts;
      const chart = createChart(container, this._createChartOptions(container));

      const candles = this._addCandlestickSeries(chart, {
        upColor: BULL_COLOR,
        downColor: BEAR_COLOR,
        borderVisible: true,
        borderUpColor: BULL_COLOR,
        borderDownColor: BEAR_COLOR,
        wickUpColor: BULL_COLOR,
        wickDownColor: BEAR_COLOR,
      });
      const volume = this._addVolumeSeries(chart, {
        priceScaleId: "volume",
        priceFormat: { type: "volume" },
        wickVisible: false,
        borderVisible: false,
        base: 0,
      });
      if (!candles || !volume) {
        chart.remove();
        return null;
      }
      chart.priceScale("volume").applyOptions({
        scaleMargins: { top: 0.72, bottom: 0 },
      });

      const tooltip = document.createElement("div");
      tooltip.className = "lw-tooltip";
      tooltip.style.display = "none";
      container.appendChild(tooltip);

      return {
        chart,
        container,
        candles,
        volume,
        tooltip,
        resizeObserver: null,
        crosshairHandler: null,
      };
    },

    _syncCombinedInstrumentPaneHeights() {
      if (!this.showInstrumentBench || this.instrumentBenchSeries.length === 0) return;
      const chart = this._priceChartCtx?.chart;
      const container = this._priceChartCtx?.container;
      if (!chart || !container || typeof chart.panes !== "function") return;
      const panes = chart.panes();
      const totalHeight = Math.max(container.clientHeight || 0, 320);
      if (typeof panes?.[0]?.setHeight === "function") panes[0].setHeight(Math.round(totalHeight * 0.58));
      if (typeof panes?.[1]?.setHeight === "function") panes[1].setHeight(Math.round(totalHeight * 0.42));
    },
    _createCombinedInstrumentChartContext(containerId) {
      const container = document.getElementById(containerId);
      if (!(container instanceof HTMLDivElement) || !window.LightweightCharts) return null;
      const { createChart } = window.LightweightCharts;
      const chart = createChart(container, this._createChartOptions(container));

      const candles = this._addCandlestickSeries(chart, {
        upColor: BULL_COLOR,
        downColor: BEAR_COLOR,
        borderVisible: true,
        borderUpColor: BULL_COLOR,
        borderDownColor: BEAR_COLOR,
        wickUpColor: BULL_COLOR,
        wickDownColor: BEAR_COLOR,
      }, 0);
      const volume = this._addVolumeSeries(chart, {
        priceScaleId: "volume",
        priceFormat: { type: "volume" },
        wickVisible: false,
        borderVisible: false,
        base: 0,
      }, 0);
      const benchCandles = this._addCandlestickSeries(chart, {
        upColor: BULL_COLOR,
        downColor: BEAR_COLOR,
        borderVisible: true,
        borderUpColor: BULL_COLOR,
        borderDownColor: BEAR_COLOR,
        wickUpColor: BULL_COLOR,
        wickDownColor: BEAR_COLOR,
      }, 1);
      const benchVolume = this._addHistogramSeries(chart, {
        priceScaleId: "bench-volume",
        priceFormat: { type: "volume" },
        priceLineVisible: false,
        lastValueVisible: false,
      }, 1);
      if (!candles || !volume || !benchCandles || !benchVolume) {
        chart.remove();
        return null;
      }
      chart.priceScale("volume").applyOptions({
        scaleMargins: { top: 0.72, bottom: 0 },
      });
      chart.priceScale("bench-volume").applyOptions({
        scaleMargins: { top: 0.72, bottom: 0 },
      });
      if (typeof chart.panes === "function") {
        const panes = chart.panes();
        const totalHeight = Math.max(container.clientHeight || 0, 320);
        if (typeof panes?.[0]?.setHeight === "function") panes[0].setHeight(Math.round(totalHeight * 0.58));
        if (typeof panes?.[1]?.setHeight === "function") panes[1].setHeight(Math.round(totalHeight * 0.42));
      }

      const tooltip = document.createElement("div");
      tooltip.className = "lw-tooltip";
      tooltip.style.display = "none";
      container.appendChild(tooltip);

      return {
        chart,
        container,
        candles,
        volume,
        benchCandles,
        benchVolume,
        tooltip,
        resizeObserver: null,
        crosshairHandler: null,
      };
    },

    _addCandlestickSeries(chart, options, paneIndex = null) {
      const lw = window.LightweightCharts || {};
      let series = null;
      if (paneIndex != null && typeof chart.addSeries === "function" && lw.CandlestickSeries) {
        series = chart.addSeries(lw.CandlestickSeries, options, paneIndex);
      } else if (typeof chart.addCandlestickSeries === "function") {
        series = chart.addCandlestickSeries(options);
      } else if (typeof chart.addSeries === "function" && lw.CandlestickSeries) {
        series = chart.addSeries(lw.CandlestickSeries, options);
      }
      if (series && paneIndex != null && typeof series.moveToPane === "function") {
        try {
          series.moveToPane(paneIndex);
        } catch (_) {}
      }
      return series;
    },

    _addVolumeSeries(chart, options, paneIndex = null) {
      return this._addCandlestickSeries(chart, options, paneIndex);
    },

    _addLineSeries(chart, options, paneIndex = null) {
      const lw = window.LightweightCharts || {};
      let series = null;
      if (paneIndex != null && typeof chart.addSeries === "function" && lw.LineSeries) {
        series = chart.addSeries(lw.LineSeries, options, paneIndex);
      } else if (typeof chart.addLineSeries === "function") {
        series = chart.addLineSeries(options);
      } else if (typeof chart.addSeries === "function" && lw.LineSeries) {
        series = chart.addSeries(lw.LineSeries, options);
      }
      if (series && paneIndex != null && typeof series.moveToPane === "function") {
        try {
          series.moveToPane(paneIndex);
        } catch (_) {}
      }
      return series;
    },

    _addHistogramSeries(chart, options, paneIndex = null) {
      const lw = window.LightweightCharts || {};
      let series = null;
      if (paneIndex != null && typeof chart.addSeries === "function" && lw.HistogramSeries) {
        series = chart.addSeries(lw.HistogramSeries, options, paneIndex);
      } else if (typeof chart.addHistogramSeries === "function") {
        series = chart.addHistogramSeries(options);
      } else if (typeof chart.addSeries === "function" && lw.HistogramSeries) {
        series = chart.addSeries(lw.HistogramSeries, options);
      }
      if (series && paneIndex != null && typeof series.moveToPane === "function") {
        try {
          series.moveToPane(paneIndex);
        } catch (_) {}
      }
      return series;
    },

    _bindInstrumentScrollPagination(ctx) {
      if (!ctx?.chart || typeof ctx.chart.timeScale !== "function") return;
      const ts = ctx.chart.timeScale();
      if (!ts || typeof ts.subscribeVisibleLogicalRangeChange !== "function") return;

      const handler = async (range) => {
        if (!range || !Number.isFinite(range.from)) return;
        if (range.from > 40) return;

        const tasks = [];
        if (this.priceHasMore && !this.priceLoading) {
          tasks.push(this.loadMorePrices({ preserveRange: true }));
        }
        if (this.showInstrumentBench && this.instrumentBenchHasMore && !this.instrumentBenchLoading) {
          tasks.push(this.loadMoreInstrumentBenchmark({ preserveRange: true }));
        }
        if (tasks.length === 0) return;
        try {
          await Promise.all(tasks);
        } catch (_) {}
      };

      ts.subscribeVisibleLogicalRangeChange(handler);
      ctx.scrollPagingUnsub = () => {
        try {
          ts.unsubscribeVisibleLogicalRangeChange(handler);
        } catch (_) {}
      };
    },

    _bindChartResize(ctx) {
      if (!ctx || !ctx.chart || !ctx.container) return;
      if (window.ResizeObserver) {
        const obs = new ResizeObserver(() => {
          const width = Math.max(ctx.container.clientWidth || 0, 320);
          const height = Math.max(ctx.container.clientHeight || 0, 320);
          ctx.chart.applyOptions({ width, height });
        });
        obs.observe(ctx.container);
        ctx.resizeObserver = obs;
      }
    },

    _disposeChartContext(ctx) {
      if (!ctx) return;
      if (ctx.resizeObserver) ctx.resizeObserver.disconnect();
      if (ctx.crosshairHandler) ctx.chart.unsubscribeCrosshairMove(ctx.crosshairHandler);
      if (ctx.scrollPagingUnsub) ctx.scrollPagingUnsub();
      if (ctx.tooltip && ctx.tooltip.parentNode) ctx.tooltip.parentNode.removeChild(ctx.tooltip);
      ctx.chart.remove();
    },

    resizeCharts() {
      const resizeOne = (ctx) => {
        if (!ctx || !ctx.chart || !ctx.container) return;
        const width = Math.max(ctx.container.clientWidth || 0, 320);
        const height = Math.max(ctx.container.clientHeight || 0, 320);
        ctx.chart.applyOptions({ width, height });
      };
      resizeOne(this._priceChartCtx);
      resizeOne(this._benchChartCtx);
      resizeOne(this._instrumentBenchChartCtx);
      this._syncCombinedInstrumentPaneHeights();
      this._syncInstrumentChartScaleWidth();
    },

    _toCandleRows(items) {
      return (Array.isArray(items) ? items : [])
        .map((row) => ({
          time: parseBusinessDay(row.trade_date),
          open: toNumber(row.open),
          high: toNumber(row.high),
          low: toNumber(row.low),
          close: toNumber(row.close),
          volume: toNumber(row.volume) ?? 0,
          trade_date: row.trade_date,
        }))
        .filter((row) => row.time && row.open != null && row.high != null && row.low != null && row.close != null);
    },

    _normalizePagedPayload(data) {
      if (Array.isArray(data)) {
        return {
          items: data,
          total: data.length,
          limit: data.length,
          offset: 0,
          has_more: false,
        };
      }
      const items = Array.isArray(data?.items) ? data.items : [];
      const rawTotal = Number(data?.total);
      const hasExplicitTotal = data?.total != null && Number.isFinite(rawTotal);
      const total = hasExplicitTotal ? rawTotal : null;
      const limit = Number.isFinite(Number(data?.limit)) ? Number(data.limit) : items.length;
      const offset = Number.isFinite(Number(data?.offset)) ? Number(data.offset) : 0;
      const hasMore = typeof data?.has_more === "boolean"
        ? data.has_more
        : (hasExplicitTotal ? (offset + items.length) < rawTotal : false);
      return {
        items,
        total,
        limit,
        offset,
        has_more: hasMore,
      };
    },

    _bindTooltip(ctx, rows, priceMASeries, volumeMASeries) {
      const format = (value, digits = 2) => {
        if (!Number.isFinite(value)) return "-";
        return Number(value).toLocaleString("ko-KR", { maximumFractionDigits: digits });
      };

      const handler = (param) => {
        if (!param || !param.point || !param.time || !param.seriesData) {
          ctx.tooltip.style.display = "none";
          return;
        }

        const candle = param.seriesData.get(ctx.candles);
        if (!candle || candle.open == null) {
          ctx.tooltip.style.display = "none";
          return;
        }

        const volume = param.seriesData.get(ctx.volume);
        const volumeValue = Number.isFinite(volume?.value) ? volume.value : volume?.close;
        const date = rows.find((r) => r.time.year === param.time.year && r.time.month === param.time.month && r.time.day === param.time.day)?.trade_date || "-";

        const priceMAs = priceMASeries
          .map((s) => {
            const value = param.seriesData.get(s.series)?.value;
            return `<div><span>${s.label}</span>: <strong>${format(value, 2)}</strong></div>`;
          })
          .join("");

        const volumeMAs = volumeMASeries
          .map((s) => {
            const value = param.seriesData.get(s.series)?.value;
            return `<div><span>V ${s.label}</span>: <strong>${format(value, 0)}</strong></div>`;
          })
          .join("");

        ctx.tooltip.innerHTML = [
          `<div class="tt-date">${date}</div>`,
          `<div>O: <strong>${format(candle.open, 2)}</strong></div>`,
          `<div>H: <strong>${format(candle.high, 2)}</strong></div>`,
          `<div>L: <strong>${format(candle.low, 2)}</strong></div>`,
          `<div>C: <strong>${format(candle.close, 2)}</strong></div>`,
          `<div>V: <strong>${format(volumeValue, 0)}</strong></div>`,
          priceMAs,
          volumeMAs,
        ].join("");

        const left = Math.min(param.point.x + 16, Math.max(0, ctx.container.clientWidth - 180));
        const top = Math.max(8, param.point.y - 12);
        ctx.tooltip.style.left = `${left}px`;
        ctx.tooltip.style.top = `${top}px`;
        ctx.tooltip.style.display = "block";
      };

      ctx.crosshairHandler = handler;
      ctx.chart.subscribeCrosshairMove(handler);
    },

    onInstrumentQueryInput() {
      if (this._instrumentSearchTimer) {
        window.clearTimeout(this._instrumentSearchTimer);
        this._instrumentSearchTimer = null;
      }

      const query = String(this.instrumentQuery || "").trim();
      if (query.length === 1) {
        this.instrumentOptions = [];
        this.instrumentHasMore = false;
        this.instrumentTotal = null;
        return;
      }

      this._instrumentSearchTimer = window.setTimeout(() => {
        this.searchInstrumentOptions();
      }, 300);
    },

    async searchInstrumentOptions() {
      if (this._instrumentSearchTimer) {
        window.clearTimeout(this._instrumentSearchTimer);
        this._instrumentSearchTimer = null;
      }
      this.instrumentOffset = 0;
      this.optionLoading = true;
      const data = await dashboardApi
        .get("/api/v1/dashboard/instrument-options", {
          q: this.instrumentQuery,
          limit: this.instrumentLimit,
          offset: this.instrumentOffset,
          include_total: false,
        })
        .catch(() => ({ items: [], total: null, limit: this.instrumentLimit, offset: 0, has_more: false }));
      const payload = this._normalizePagedPayload(data);
      this.instrumentOptions = payload.items;
      this.instrumentTotal = payload.total;
      this.instrumentHasMore = payload.has_more;
      this.optionLoading = false;
      if (!this.selectedInstrument && this.instrumentOptions.length > 0) {
        await this.selectInstrument(this.instrumentOptions[0]);
      }
    },

    async loadMoreInstrumentOptions() {
      if (this.optionLoading || !this.instrumentHasMore) return;
      this.optionLoading = true;
      const nextOffset = this.instrumentOffset + this.instrumentLimit;
      const data = await dashboardApi
        .get("/api/v1/dashboard/instrument-options", {
          q: this.instrumentQuery,
          limit: this.instrumentLimit,
          offset: nextOffset,
          include_total: false,
        })
        .catch(() => ({ items: [], total: this.instrumentTotal, limit: this.instrumentLimit, offset: nextOffset, has_more: false }));
      const payload = this._normalizePagedPayload(data);
      this.instrumentOptions = this.instrumentOptions.concat(payload.items);
      this.instrumentOffset = nextOffset;
      this.instrumentTotal = payload.total;
      this.instrumentHasMore = payload.has_more;
      this.optionLoading = false;
    },

    async selectInstrument(option) {
      this.selectedInstrument = option;
      this.instrumentProfile = null;
      this.prices = [];
      this.priceTotal = null;
      this.priceOffset = 0;
      this.priceHasMore = false;
      this.priceQueried = false;
      this._disposeChartContext(this._priceChartCtx);
      this._priceChartCtx = null;
      this.resetInstrumentBenchmark();
      await Promise.all([this.loadInstrumentProfile(), this.loadPrices()]);
    },

    async loadInstrumentProfile() {
      if (!this.selectedInstrument?.external_code) return;
      const code = encodeURIComponent(this.selectedInstrument.external_code);
      const data = await dashboardApi
        .get(`/api/v1/dashboard/instruments/${code}/profile`)
        .catch(() => ({}));
      this.instrumentProfile = data || {};
    },

    async _fetchAllPaged(path, params, fallbackLimit) {
      const items = [];
      let total = 0;
      let offset = 0;
      let hasMore = false;

      while (true) {
        const data = await dashboardApi
          .get(path, {
            ...params,
            limit: fallbackLimit,
            offset,
          })
          .catch(() => ({ items: [], total: 0, limit: fallbackLimit, offset, has_more: false }));
        const payload = this._normalizePagedPayload(data);
        items.push(...payload.items);
        total = payload.total;
        hasMore = payload.has_more;
        if (!payload.has_more || payload.items.length === 0) break;
        offset += fallbackLimit;
      }

      return { items, total, offset, hasMore };
    },

    _focusChartOnRecentYear(chart, rows) {
      const timeScale = chart?.timeScale?.();
      if (!timeScale || !Array.isArray(rows) || rows.length === 0) return;

      const approxTradingDaysInYear = 252;
      const visibleBars = Math.min(rows.length, approxTradingDaysInYear);
      const from = Math.max(0, rows.length - visibleBars);
      const to = rows.length - 1 + 5;

      if (typeof timeScale.setVisibleLogicalRange === "function") {
        try {
          timeScale.setVisibleLogicalRange({ from, to });
          return;
        } catch (_) {}
      }

      if (typeof timeScale.setVisibleRange === "function") {
        const last = rows[rows.length - 1]?.time;
        const first = rows[from]?.time;
        if (first && last) {
          try {
            timeScale.setVisibleRange({ from: first, to: last });
            return;
          } catch (_) {}
        }
      }

      if (typeof timeScale.fitContent === "function") timeScale.fitContent();
    },

    async loadPrices() {
      if (!this.selectedInstrument?.external_code) return;
      const requestSeq = ++this._priceRequestSeq;
      this.priceOffset = 0;
      this.priceLoading = true;
      this.priceQueried = true;

      const data = await dashboardApi
        .get("/api/v1/dashboard/prices", {
          external_code: this.selectedInstrument.external_code,
          limit: this.priceLimit,
          offset: 0,
          include_total: false,
        })
        .catch(() => ({ items: [], total: null, limit: this.priceLimit, offset: 0, has_more: false }));
      if (requestSeq !== this._priceRequestSeq) return;

      const payload = this._normalizePagedPayload(data);
      this.prices = payload.items.slice().reverse();
      this.priceTotal = payload.total;
      this.priceOffset = payload.offset;
      this.priceHasMore = payload.has_more;
      this.priceLoading = false;
      await this.$nextTick();
      this.renderPriceChart();
      if (this.showInstrumentBench) {
        await this.loadInstrumentBenchmark();
      }
    },

    async loadMorePrices(options = {}) {
      if (!this.selectedInstrument?.external_code) return;
      if (this.priceLoading || !this.priceHasMore) return;

      const preserveRange = options?.preserveRange === true;
      const requestSeq = ++this._priceRequestSeq;
      const nextOffset = this.priceOffset + this.priceLimit;
      this.priceLoading = true;

      const data = await dashboardApi
        .get("/api/v1/dashboard/prices", {
          external_code: this.selectedInstrument.external_code,
          limit: this.priceLimit,
          offset: nextOffset,
          include_total: false,
        })
        .catch(() => ({ items: [], total: this.priceTotal, limit: this.priceLimit, offset: nextOffset, has_more: false }));
      if (requestSeq !== this._priceRequestSeq) return;

      const payload = this._normalizePagedPayload(data);
      const olderChunk = payload.items.slice().reverse();
      this.prices = olderChunk.concat(this.prices);
      this.priceOffset = nextOffset;
      this.priceHasMore = payload.has_more;
      if (payload.total != null) this.priceTotal = payload.total;
      this.priceLoading = false;

      await this.$nextTick();
      this.renderPriceChart({ preserveRange });
    },

    resolveInstrumentBenchmarkIndexCode(marketCode) {
      const normalized = String(marketCode || "").toUpperCase();
      if (normalized === "KOSPI") return "KOSPI";
      if (normalized === "KOSDAQ") return "KOSDAQ";
      if (normalized === "KONEX") return "KRX";
      return "KRX";
    },

    resolveInstrumentBenchmarkDefaultSeriesName(marketCode) {
      const normalized = String(marketCode || "").toUpperCase();
      if (normalized === "KOSDAQ") return "코스닥 150";
      if (normalized === "KOSPI") return "코스피 200";
      return "";
    },

    resetInstrumentBenchmark() {
      this._instrumentBenchRequestSeq += 1;
      this._clearInstrumentChartSync();
      this.instrumentBenchSeries = [];
      this.instrumentBenchLoading = false;
      this.instrumentBenchQueried = false;
      this.instrumentBenchError = "";
      this.instrumentBenchIndexCode = "";
      this.instrumentBenchSeriesName = "";
      this.instrumentBenchSeriesOptions = [];
      this.instrumentBenchTotal = null;
      this.instrumentBenchOffset = 0;
      this.instrumentBenchHasMore = false;
      this._disposeChartContext(this._instrumentBenchChartCtx);
      this._instrumentBenchChartCtx = null;
    },

    _instrumentBenchmarkIndexChoices() {
      const rows = Array.isArray(this.benchmarks) ? this.benchmarks : [];
      return rows.map((row) => String(row.index_code || "").trim()).filter((code) => code.length > 0);
    },

    ensureInstrumentBenchmarkIndexDefault() {
      const choices = this._instrumentBenchmarkIndexChoices();
      const marketCode = this.instrumentProfile?.market_code || this.selectedInstrument?.market_code;
      const defaultCode = this.resolveInstrumentBenchmarkIndexCode(marketCode);
      if (choices.includes(defaultCode)) {
        this.instrumentBenchIndexCode = defaultCode;
        return;
      }
      this.instrumentBenchIndexCode = choices[0] || defaultCode;
    },

    async onInstrumentBenchmarkToggle() {
      if (!this.showInstrumentBench) {
        this.resetInstrumentBenchmark();
        await this.$nextTick();
        this.renderPriceChart();
        return;
      }
      if (!this.instrumentBenchIndexCode) {
        this.ensureInstrumentBenchmarkIndexDefault();
      }
      await this.loadInstrumentBenchmark();
    },

    async onInstrumentBenchmarkIndexChange() {
      if (!this.showInstrumentBench) return;
      this.instrumentBenchSeriesName = "";
      this.instrumentBenchSeriesOptions = [];
      await this.loadInstrumentBenchmark();
    },

    async onInstrumentBenchmarkSeriesChange() {
      if (!this.showInstrumentBench) return;
      await this.loadInstrumentBenchmark();
    },

    async loadInstrumentBenchmark(options = {}) {
      if (!this.showInstrumentBench || !this.selectedInstrument) return;
      if (!this.instrumentBenchIndexCode) {
        this.ensureInstrumentBenchmarkIndexDefault();
      }
      const preserveRange = options?.preserveRange === true;
      const requestSeq = ++this._instrumentBenchRequestSeq;
      this.instrumentBenchLoading = true;
      this.instrumentBenchQueried = true;
      this.instrumentBenchError = "";
      this.instrumentBenchSeries = [];
      this.instrumentBenchTotal = null;
      this.instrumentBenchOffset = 0;
      this.instrumentBenchHasMore = false;
      this._disposeChartContext(this._instrumentBenchChartCtx);
      this._instrumentBenchChartCtx = null;
      const indexCode = this.instrumentBenchIndexCode;

      const seriesOptions = await dashboardApi
        .get("/api/v1/dashboard/benchmark-series", { index_code: indexCode })
        .catch(() => []);
      if (requestSeq !== this._instrumentBenchRequestSeq) return;

      const optionsRows = Array.isArray(seriesOptions) ? seriesOptions : [];
      this.instrumentBenchSeriesOptions = optionsRows;
      if (optionsRows.length === 0) {
        this.instrumentBenchLoading = false;
        this.instrumentBenchError = "선택된 시장에 대한 벤치마크 시리즈를 찾지 못했습니다.";
        return;
      }
      const hasSelected = optionsRows.some((row) => row.index_name === this.instrumentBenchSeriesName);
      if (!hasSelected) {
        const marketCode = this.instrumentProfile?.market_code || this.selectedInstrument?.market_code;
        const defaultSeriesName = this.resolveInstrumentBenchmarkDefaultSeriesName(marketCode);
        const exactDefault = optionsRows.find((row) => String(row?.index_name || "").trim() === defaultSeriesName);
        this.instrumentBenchSeriesName = exactDefault?.index_name || optionsRows[0].index_name;
      }

      const data = await dashboardApi
        .get(`/api/v1/dashboard/benchmarks/${encodeURIComponent(indexCode)}`, {
          series_name: this.instrumentBenchSeriesName,
          limit: this.instrumentBenchLimit,
          offset: 0,
          include_total: false,
        })
        .catch(() => ({ items: [], total: null, limit: this.instrumentBenchLimit, offset: 0, has_more: false }));
      if (requestSeq !== this._instrumentBenchRequestSeq) return;

      const payload = this._normalizePagedPayload(data);
      this.instrumentBenchSeries = payload.items.slice().reverse();
      this.instrumentBenchTotal = payload.total;
      this.instrumentBenchOffset = payload.offset;
      this.instrumentBenchHasMore = payload.has_more;
      this.instrumentBenchLoading = false;
      await this.$nextTick();
      this.renderPriceChart({ preserveRange });
    },

    async loadMoreInstrumentBenchmark(options = {}) {
      if (!this.showInstrumentBench || !this.selectedInstrument) return;
      if (this.instrumentBenchLoading || !this.instrumentBenchHasMore) return;

      const preserveRange = options?.preserveRange === true;
      const requestSeq = ++this._instrumentBenchRequestSeq;
      const nextOffset = this.instrumentBenchOffset + this.instrumentBenchLimit;
      this.instrumentBenchLoading = true;

      const data = await dashboardApi
        .get(`/api/v1/dashboard/benchmarks/${encodeURIComponent(this.instrumentBenchIndexCode)}`, {
          series_name: this.instrumentBenchSeriesName,
          limit: this.instrumentBenchLimit,
          offset: nextOffset,
          include_total: false,
        })
        .catch(() => ({ items: [], total: this.instrumentBenchTotal, limit: this.instrumentBenchLimit, offset: nextOffset, has_more: false }));
      if (requestSeq !== this._instrumentBenchRequestSeq) return;

      const payload = this._normalizePagedPayload(data);
      const olderChunk = payload.items.slice().reverse();
      this.instrumentBenchSeries = olderChunk.concat(this.instrumentBenchSeries);
      this.instrumentBenchOffset = nextOffset;
      this.instrumentBenchHasMore = payload.has_more;
      if (payload.total != null) this.instrumentBenchTotal = payload.total;
      this.instrumentBenchLoading = false;

      await this.$nextTick();
      this.renderPriceChart({ preserveRange });
    },

    renderInstrumentBenchChart() {
      this.renderPriceChart();
    },

    renderPriceChart(options = {}) {
      const preserveRange = options?.preserveRange === true;
      let prevVisibleRange = null;
      let prevLogicalRange = null;
      if (preserveRange && this._priceChartCtx?.chart?.timeScale) {
        const prevTs = this._priceChartCtx.chart.timeScale();
        if (prevTs) {
          if (typeof prevTs.getVisibleRange === "function") prevVisibleRange = prevTs.getVisibleRange();
          if (typeof prevTs.getVisibleLogicalRange === "function") prevLogicalRange = prevTs.getVisibleLogicalRange();
        }
      }

      this._clearInstrumentChartSync();
      this._disposeChartContext(this._priceChartCtx);
      this._priceChartCtx = null;
      this.priceChartError = "";
      if (!this.prices.length) return;

      try {
        const rows = this._toCandleRows(this.prices);
        if (!rows.length) {
          this.priceChartError = "유효한 OHLC 데이터가 없어 캔들차트를 그릴 수 없습니다.";
          return;
        }

        const showCombined = this.showInstrumentBench && this.instrumentBenchSeries.length > 0;
        const benchRows = showCombined ? this._toCandleRows(this.instrumentBenchSeries) : [];
        const ctx = showCombined ? this._createCombinedInstrumentChartContext("priceChart") : this._createChartContext("priceChart");
        if (!ctx) {
          this.priceChartError = showCombined ? "통합 차트를 생성하지 못했습니다." : "차트를 렌더링하지 못했습니다.";
          return;
        }
        ctx.baseRows = rows;
        ctx.timeKeyMap = new Map(rows.map((row) => [formatCompactBusinessDay(row.time), row]));

        ctx.candles.setData(rows.map((r) => ({ time: r.time, open: r.open, high: r.high, low: r.low, close: r.close })));
        ctx.volume.setData(rows.map((r) => toVolumeCandlePoint(r, BULL_VOLUME_COLOR)));

        const priceMASeries = this.priceMAPeriods.map((period, idx) => {
          const series = this._addLineSeries(ctx.chart, {
            color: getMAColor(idx),
            lineWidth: 1.5,
            priceLineVisible: false,
            lastValueVisible: false,
          }, showCombined ? 0 : null);
          if (!series) return null;
          series.setData(computeSMA(rows, period, (r) => r.close));
          return { label: `SMA ${period}`, period, series };
        }).filter(Boolean);

        const volumeMASeries = this.volumeMAPeriods.map((period, idx) => {
          const series = this._addLineSeries(ctx.chart, {
            color: getMAColor(idx + this.priceMAPeriods.length),
            lineWidth: 1.2,
            priceScaleId: "volume",
            priceLineVisible: false,
            lastValueVisible: false,
          }, showCombined ? 0 : null);
          if (!series) return null;
          series.setData(computeSMA(rows, period, (r) => r.volume));
          return { label: `SMA ${period}`, period, series };
        }).filter(Boolean);

        if (showCombined && ctx.benchCandles && ctx.benchVolume) {
          if (!benchRows.length) {
            this.priceChartError = "벤치마크 OHLC 데이터가 없어 통합 차트를 그릴 수 없습니다.";
            this._disposeChartContext(ctx);
            return;
          }
          ctx.benchRows = benchRows;
          ctx.benchTimeKeyMap = new Map(benchRows.map((row) => [formatCompactBusinessDay(row.time), row]));
          ctx.benchCandles.setData(benchRows.map((r) => ({ time: r.time, open: r.open, high: r.high, low: r.low, close: r.close })));
          ctx.benchVolume.setData(benchRows.map((r) => toVolumeHistogramPoint(r, BULL_VOLUME_COLOR)));
        }

        this._bindTooltip(ctx, rows, priceMASeries, volumeMASeries);
        this._bindChartResize(ctx);
        this._bindInstrumentScrollPagination(ctx);

        if (preserveRange) {
          const ts = ctx.chart?.timeScale?.();
          if (ts) {
            let restored = false;
            if (prevVisibleRange && typeof ts.setVisibleRange === "function") {
              try {
                ts.setVisibleRange(prevVisibleRange);
                restored = true;
              } catch (_) {}
            }
            if (!restored && prevLogicalRange && typeof ts.setVisibleLogicalRange === "function") {
              try {
                ts.setVisibleLogicalRange(prevLogicalRange);
                restored = true;
              } catch (_) {}
            }
            if (!restored) this._focusChartOnRecentYear(ctx.chart, rows);
          } else {
            this._focusChartOnRecentYear(ctx.chart, rows);
          }
        } else {
          this._focusChartOnRecentYear(ctx.chart, rows);
        }

        this._priceChartCtx = ctx;
        this._syncCombinedInstrumentPaneHeights();
      } catch (err) {
        this._disposeChartContext(this._priceChartCtx);
        this._priceChartCtx = null;
        console.error("renderPriceChart failed", err);
        this.priceChartError = "차트를 렌더링하지 못했습니다. 표 데이터로 확인해 주세요.";
      }
    },

    async loadSummary() {
      this.summary = await dashboardApi.get("/api/v1/dashboard/summary").catch(() => ({}));
    },

    async loadRuns() {
      this.runsLoading = true;
      this.runs = await dashboardApi.get("/api/v1/dashboard/runs", { limit: 20 }).catch(() => []);
      this.runsLoading = false;
    },

    async loadBenchmarks() {
      this.benchmarks = await dashboardApi.get("/api/v1/dashboard/benchmarks").catch(() => []);
    },

    async onBenchmarkIndexChange() {
      this.selectedSeries = "";
      this.benchSeries = [];
      this.benchSeriesOptions = [];
      this.benchTotal = 0;
      this.benchOffset = 0;
      this._disposeChartContext(this._benchChartCtx);
      this._benchChartCtx = null;
      if (!this.selectedIndex) return;
      this.benchSeriesOptions = await dashboardApi
        .get("/api/v1/dashboard/benchmark-series", { index_code: this.selectedIndex })
        .catch(() => []);
      if (this.benchSeriesOptions.length > 0) {
        this.selectedSeries = this.benchSeriesOptions[0].index_name;
        await this.loadBenchmarkSeries();
      }
    },

    async loadBenchmarkSeries() {
      if (!this.selectedIndex || !this.selectedSeries) return;
      this.benchLoading = true;
      const data = await dashboardApi
        .get(`/api/v1/dashboard/benchmarks/${encodeURIComponent(this.selectedIndex)}`, {
          series_name: this.selectedSeries,
          date_from: this.benchFrom,
          date_to: this.benchTo,
          limit: this.benchLimit,
          offset: this.benchOffset,
        })
        .catch(() => ({ items: [] }));
      this.benchTotal = data.total || 0;
      this.benchSeries = (data.items || []).slice().reverse();
      this.benchLoading = false;
      await this.$nextTick();
      this.renderBenchChart();
    },

    renderBenchChart() {
      this._disposeChartContext(this._benchChartCtx);
      this._benchChartCtx = null;
      this.benchChartError = "";
      if (!this.benchSeries.length) return;

      try {
        const ctx = this._createChartContext("benchChart");
        if (!ctx) return;
        const rows = this._toCandleRows(this.benchSeries);
        if (!rows.length) {
          this.benchChartError = "유효한 OHLC 데이터가 없어 캔들차트를 그릴 수 없습니다.";
          return;
        }

        ctx.candles.setData(rows.map((r) => ({ time: r.time, open: r.open, high: r.high, low: r.low, close: r.close })));
        ctx.volume.setData(rows.map((r) => toVolumeCandlePoint(r, BULL_VOLUME_COLOR)));

        const priceMASeries = this.priceMAPeriods.map((period, idx) => {
          const series = this._addLineSeries(ctx.chart, {
            color: getMAColor(idx),
            lineWidth: 1.5,
            priceLineVisible: false,
            lastValueVisible: false,
          });
          if (!series) return null;
          series.setData(computeSMA(rows, period, (r) => r.close));
          return { label: `SMA ${period}`, period, series };
        }).filter(Boolean);

        const volumeMASeries = this.volumeMAPeriods.map((period, idx) => {
          const series = this._addLineSeries(ctx.chart, {
            color: getMAColor(idx + this.priceMAPeriods.length),
            lineWidth: 1.2,
            priceScaleId: "volume",
            priceLineVisible: false,
            lastValueVisible: false,
          });
          if (!series) return null;
          series.setData(computeSMA(rows, period, (r) => r.volume));
          return { label: `SMA ${period}`, period, series };
        }).filter(Boolean);

        this._bindTooltip(ctx, rows, priceMASeries, volumeMASeries);
        this._bindChartResize(ctx);
        ctx.chart.timeScale().fitContent();
        this._benchChartCtx = ctx;
      } catch (err) {
        this._disposeChartContext(this._benchChartCtx);
        this._benchChartCtx = null;
        console.error("renderBenchChart failed", err);
        this.benchChartError = "차트를 렌더링하지 못했습니다.";
      }
    },

    async loadQualityIssues() {
      this.qualityLoading = true;
      this.qualityIssues = await dashboardApi
        .get("/api/v1/dashboard/quality-issues", {
          limit: 50,
          severity: this.qualitySeverity,
        })
        .catch(() => []);
      this.qualityLoading = false;
    },

    fmt(n) {
      if (n == null || n === "-") return "-";
      if (typeof n === "number") return n.toLocaleString("ko-KR");
      const num = Number(n);
      return Number.isNaN(num) ? n : num.toLocaleString("ko-KR");
    },

    elapsed(start, end) {
      if (!start || !end) return "-";
      const diff = (new Date(end) - new Date(start)) / 1000;
      return diff.toFixed(1);
    },

    statusBadge(status) {
      const map = {
        SUCCESS: "badge badge-success",
        PARTIAL: "badge badge-partial",
        FAILED: "badge badge-failed",
        RUNNING: "badge badge-running",
      };
      return map[status] || "badge";
    },

    severityBadge(sev) {
      const map = { ERROR: "badge badge-error", WARN: "badge badge-warn", INFO: "badge badge-info" };
      return map[sev] || "badge";
    },
  };
}



















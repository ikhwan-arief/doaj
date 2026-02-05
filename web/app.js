const API_BASE = window.DOAJ_API_BASE || "http://localhost:8001";
const statusEl = document.getElementById("dataStatus");
const summaryEls = {
  total: document.getElementById("totalCount"),
  lastUpdate: document.getElementById("lastUpdate"),
  topCountry: document.getElementById("topCountry"),
};
const metricSelect = document.getElementById("metricSelect");

const charts = new Map();
const formatNumber = new Intl.NumberFormat("en-US");

const countryAliases = {
  "United States": "United States of America",
  "Russia": "Russian Federation",
  "Iran": "Iran (Islamic Republic of)",
  "Syria": "Syrian Arab Republic",
  "Venezuela": "Venezuela (Bolivarian Republic of)",
  "Tanzania": "United Republic of Tanzania",
  "Bolivia": "Bolivia (Plurinational State of)",
};

async function fetchJSON(path) {
  const response = await fetch(`${API_BASE}${path}`);
  if (!response.ok) {
    throw new Error(`Request failed: ${response.status}`);
  }
  return response.json();
}

function normalizeMetric(items = []) {
  return items
    .filter((item) => item && item.key)
    .map((item) => ({
      name: String(item.key),
      value: Number(item.value) || 0,
    }));
}

function renderSummary(summary, metrics) {
  summaryEls.total.textContent = formatNumber.format(summary.total || 0);
  summaryEls.lastUpdate.textContent = summary.generated_at
    ? new Date(summary.generated_at).toLocaleString()
    : "--";

  const topCountry = (metrics.by_country || [])[0]?.key || "--";
  summaryEls.topCountry.textContent = topCountry;

  if (summary.source === "sample") {
    statusEl.textContent = "Showing sample data (run ingestion for live metrics).";
  } else {
    statusEl.textContent = `Data source: ${summary.source || "unknown"}`;
  }
}

function renderMap(metrics) {
  const data = normalizeMetric(metrics.by_country).map((item) => ({
    name: countryAliases[item.name] || item.name,
    value: item.value,
  }));

  const chart = charts.get("map") || echarts.init(document.getElementById("mapChart"));
  chart.setOption({
    tooltip: { trigger: "item" },
    visualMap: {
      min: 0,
      max: Math.max(1, ...data.map((item) => item.value)),
      left: "left",
      bottom: "5%",
      text: ["High", "Low"],
      inRange: { color: ["#f4e4c6", "#d08c3f"] },
    },
    series: [
      {
        type: "map",
        map: "world",
        roam: true,
        emphasis: { label: { show: false } },
        data,
      },
    ],
  });
  charts.set("map", chart);
}

function renderTimeline(metrics) {
  const data = normalizeMetric(metrics.by_year)
    .filter((item) => item.name)
    .sort((a, b) => Number(a.name) - Number(b.name));

  const chart = charts.get("timeline") || echarts.init(document.getElementById("timelineChart"));
  chart.setOption({
    tooltip: { trigger: "axis" },
    xAxis: {
      type: "category",
      data: data.map((item) => item.name),
      boundaryGap: false,
    },
    yAxis: { type: "value" },
    series: [
      {
        type: "line",
        data: data.map((item) => item.value),
        smooth: true,
        areaStyle: { color: "rgba(17, 131, 125, 0.2)" },
        lineStyle: { color: "#11837d", width: 3 },
      },
    ],
  });
  charts.set("timeline", chart);
}

function renderPie(metrics) {
  const data = normalizeMetric(metrics.by_license);
  const chart = charts.get("pie") || echarts.init(document.getElementById("pieChart"));
  chart.setOption({
    tooltip: { trigger: "item" },
    series: [
      {
        type: "pie",
        radius: ["45%", "70%"],
        data,
        label: { color: "#3a372f" },
      },
    ],
  });
  charts.set("pie", chart);
}

function renderBar(metrics, key) {
  const data = normalizeMetric(metrics[key]).slice(0, 12).reverse();
  const chart = charts.get("bar") || echarts.init(document.getElementById("barChart"));
  chart.setOption({
    tooltip: { trigger: "axis" },
    grid: { left: "10%", right: "6%", top: 10, bottom: 10, containLabel: true },
    xAxis: { type: "value" },
    yAxis: { type: "category", data: data.map((item) => item.name) },
    series: [
      {
        type: "bar",
        data: data.map((item) => item.value),
        itemStyle: { color: "#d97745" },
      },
    ],
  });
  charts.set("bar", chart);
}

function renderSubjects(metrics) {
  const data = normalizeMetric(metrics.by_subject).map((item) => ({
    name: item.name,
    value: item.value,
  }));
  const chart = charts.get("subject") || echarts.init(document.getElementById("subjectChart"));
  chart.setOption({
    tooltip: { trigger: "item" },
    series: [
      {
        type: "treemap",
        data,
        leafDepth: 1,
        label: { show: true, formatter: "{b}" },
        upperLabel: { show: false },
      },
    ],
  });
  charts.set("subject", chart);
}

function renderPublishers(metrics) {
  const data = normalizeMetric(metrics.top_publishers).slice(0, 10).reverse();
  const chart = charts.get("publisher") || echarts.init(document.getElementById("publisherChart"));
  chart.setOption({
    tooltip: { trigger: "axis" },
    grid: { left: "12%", right: "6%", top: 10, bottom: 10, containLabel: true },
    xAxis: { type: "value" },
    yAxis: { type: "category", data: data.map((item) => item.name) },
    series: [
      {
        type: "bar",
        data: data.map((item) => item.value),
        itemStyle: { color: "#0f766e" },
      },
    ],
  });
  charts.set("publisher", chart);
}

function attachResize() {
  window.addEventListener("resize", () => {
    charts.forEach((chart) => chart.resize());
  });
}

async function initDashboard() {
  try {
    const [summary, metrics] = await Promise.all([
      fetchJSON("/api/summary"),
      fetchJSON("/api/metrics"),
    ]);

    renderSummary(summary, metrics);
    renderMap(metrics);
    renderTimeline(metrics);
    renderPie(metrics);
    renderSubjects(metrics);
    renderPublishers(metrics);
    renderBar(metrics, metricSelect.value);

    metricSelect.addEventListener("change", () => renderBar(metrics, metricSelect.value));
    attachResize();
  } catch (error) {
    statusEl.textContent = "Unable to reach the API. Start the backend at http://localhost:8001.";
    console.error(error);
  }
}

initDashboard();

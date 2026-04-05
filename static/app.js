const hoursInput = document.querySelector("#hours");
const windowSlider = document.querySelector("#window-slider");
const startDateInput = document.querySelector("#start-date");
const startTimeInput = document.querySelector("#start-time");
const statusCards = document.querySelector("#status-cards");
const latestValues = document.querySelector("#latest-values");
const cumulativeStats = document.querySelector("#cumulative-stats");
const refreshText = document.querySelector("#last-refresh");
const themeToggle = document.querySelector("#theme-toggle");
const chartElement = document.querySelector("#chart");
const netChartElement = document.querySelector("#net-chart");
const cumulativeChartElement = document.querySelector("#cumulative-chart");

function getTheme() {
  return document.documentElement.dataset.theme || "light";
}

function setTheme(theme) {
  document.documentElement.dataset.theme = theme;
  localStorage.setItem("solar-monitor-theme", theme);
  themeToggle.textContent = theme === "dark" ? "Light mode" : "Dark mode";
}

function buildLocalDateTime(dateValue, timeValue) {
  if (!dateValue) {
    return new Date();
  }

  const [year, month, day] = dateValue.split("-").map(Number);
  const [hours, minutes] = (timeValue || "00:00").split(":").map(Number);
  return new Date(year, (month || 1) - 1, day || 1, hours || 0, minutes || 0, 0, 0);
}

function buildChartTheme() {
  const dark = getTheme() === "dark";
  return {
    paper_bgcolor: "rgba(0,0,0,0)",
    plot_bgcolor: dark ? "#132134" : "#ffffff",
    margin: { t: 18, r: 18, b: 44, l: 52 },
    hovermode: "x unified",
    hoverlabel: {
      bgcolor: dark ? "rgba(13, 22, 35, 0.96)" : "rgba(255,255,255,0.95)",
      bordercolor: dark ? "rgba(135, 156, 186, 0.22)" : "rgba(120, 132, 155, 0.22)",
      font: { color: dark ? "#edf4ff" : "#314055", size: 12 }
    },
    xaxis: {
      title: "",
      gridcolor: dark ? "rgba(124, 147, 180, 0.12)" : "rgba(164, 179, 201, 0.14)",
      tickfont: { color: dark ? "#97abc5" : "#7a8797", size: 11 },
      zeroline: false,
      linecolor: dark ? "rgba(124, 147, 180, 0.18)" : "rgba(164, 179, 201, 0.18)"
    },
    yaxis: {
      title: "",
      gridcolor: dark ? "rgba(124, 147, 180, 0.14)" : "rgba(164, 179, 201, 0.16)",
      tickfont: { color: dark ? "#97abc5" : "#7a8797", size: 11 },
      zeroline: false,
      rangemode: "tozero"
    },
    legend: {
      orientation: "h",
      y: -0.14,
      x: 0,
      font: { color: dark ? "#c5d3e6" : "#5e6b7d", size: 12 },
      bgcolor: "rgba(0,0,0,0)"
    }
  };
}

function formatNumber(value) {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "n/a";
  }
  return `${Number(value).toFixed(1)} W`;
}

function formatStatusCard(item) {
  const details = Object.entries(item.details || {})
    .map(([key, value]) => `<small><strong>${key}</strong>: ${value ?? "n/a"}</small>`)
    .join("");

  const errorBlock = item.last_error
    ? `<small><strong>last error</strong>: ${item.last_error}</small>`
    : "";

  return `
    <details class="status-card">
      <summary>
        <strong>${item.name}</strong>
        <span class="status-pill status-${item.state}">${item.state}</span>
      </summary>
      <div class="status-minimal">
        <small>${item.last_success_at ?? "never"}</small>
        <small>${item.last_error ? "Issue recorded" : "Healthy"}</small>
      </div>
      <div class="status-card-body">
        <small><strong>last success</strong>: ${item.last_success_at ?? "never"}</small>
        ${errorBlock}
        ${details}
      </div>
    </details>
  `;
}

function formatMetricCard(item) {
  const isImputed = item.raw_payload && item.raw_payload.imputed;
  return `
    <article class="metric-card">
      <span>${item.source}</span>
      <strong>Grid: ${formatNumber(item.grid_usage_watts)}</strong>
      <strong>Solar: ${formatNumber(item.solar_generation_watts)}</strong>
      <small>${isImputed ? "Estimated from previous readings" : "Live reading"}</small>
      <small>${item.observed_at}</small>
    </article>
  `;
}

function formatStatCard(label, value, detail) {
  return `
    <article class="stat-card">
      <span>${label}</span>
      <strong>${value}</strong>
      <small>${detail}</small>
    </article>
  `;
}

function buildNetItems(items) {
  const solarItems = items
    .filter((item) => item.source === "local_site" && item.solar_generation_watts !== null)
    .sort((left, right) => new Date(left.observed_at) - new Date(right.observed_at));

  const bleGridItems = items
    .filter((item) => item.source === "ble" && item.grid_usage_watts !== null)
    .sort((left, right) => new Date(left.observed_at) - new Date(right.observed_at));

  const netItems = [];
  let bleIndex = 0;

  for (const solarItem of solarItems) {
    const solarTime = new Date(solarItem.observed_at).getTime();

    while (
      bleIndex + 1 < bleGridItems.length &&
      new Date(bleGridItems[bleIndex + 1].observed_at).getTime() <= solarTime
    ) {
      bleIndex += 1;
    }

    const gridItem = bleGridItems[bleIndex];
    if (!gridItem) {
      continue;
    }

    const gridTime = new Date(gridItem.observed_at).getTime();
    if (gridTime > solarTime) {
      continue;
    }

    netItems.push({
      observed_at: solarItem.observed_at,
      solar_generation_watts: Number(solarItem.solar_generation_watts),
      grid_usage_watts: Number(gridItem.grid_usage_watts),
      net_power_watts: Number(solarItem.solar_generation_watts) - Number(gridItem.grid_usage_watts),
      imputed: Boolean((solarItem.raw_payload && solarItem.raw_payload.imputed) || (gridItem.raw_payload && gridItem.raw_payload.imputed))
    });
  }

  return netItems;
}

function integrateSeriesKwh(series, valueKey) {
  let cumulative = 0;
  const points = [];
  let currentDayKey = null;

  for (let index = 0; index < series.length; index += 1) {
    const current = series[index];
    const currentDate = new Date(current.observed_at);
    const dayKey = `${currentDate.getFullYear()}-${currentDate.getMonth()}-${currentDate.getDate()}`;

    if (dayKey !== currentDayKey) {
      cumulative = 0;
      currentDayKey = dayKey;
    }

    if (index > 0) {
      const previous = series[index - 1];
      const previousDate = new Date(previous.observed_at);
      const previousDayKey = `${previousDate.getFullYear()}-${previousDate.getMonth()}-${previousDate.getDate()}`;
      const deltaHours = (currentDate - previousDate) / 3600000;
      const averageWatts = (Number(previous[valueKey]) + Number(current[valueKey])) / 2;
      if (deltaHours > 0 && previousDayKey === dayKey) {
        cumulative += (averageWatts / 1000) * deltaHours;
      }
    }

    points.push({
      observed_at: current.observed_at,
      cumulative_kwh: cumulative
    });
  }

  return points;
}

function computeRampSeries(series, valueKey) {
  const points = [];
  for (let index = 1; index < series.length; index += 1) {
    const previous = series[index - 1];
    const current = series[index];
    const deltaMinutes = (new Date(current.observed_at) - new Date(previous.observed_at)) / 60000;
    if (deltaMinutes <= 0) {
      continue;
    }

    points.push({
      observed_at: current.observed_at,
      rate_w_per_min: (Number(current[valueKey]) - Number(previous[valueKey])) / deltaMinutes,
      rate_kw_per_hr: ((Number(current[valueKey]) - Number(previous[valueKey])) / deltaMinutes) * 60 / 1000
    });
  }
  return points;
}

function renderCumulativeStats(items) {
  const solarItems = items
    .filter((item) => item.source === "local_site" && item.solar_generation_watts !== null)
    .sort((left, right) => new Date(left.observed_at) - new Date(right.observed_at));
  const bleGridItems = items
    .filter((item) => item.source === "ble" && item.grid_usage_watts !== null)
    .sort((left, right) => new Date(left.observed_at) - new Date(right.observed_at));
  const netItems = buildNetItems(items);

  const solarKwh = integrateSeriesKwh(solarItems, "solar_generation_watts");
  const gridKwh = integrateSeriesKwh(bleGridItems, "grid_usage_watts");
  const exportKwh = integrateSeriesKwh(
    netItems.map((item) => ({
      observed_at: item.observed_at,
      net_power_positive: Math.max(item.net_power_watts, 0)
    })),
    "net_power_positive"
  );

  const imputedCount = items.filter((item) => item.raw_payload && item.raw_payload.imputed).length;
  const latestSolarKwh = solarKwh.length ? solarKwh[solarKwh.length - 1].cumulative_kwh : 0;
  const latestGridKwh = gridKwh.length ? gridKwh[gridKwh.length - 1].cumulative_kwh : 0;
  const latestExportKwh = exportKwh.length ? exportKwh[exportKwh.length - 1].cumulative_kwh : 0;

  cumulativeStats.innerHTML = [
    formatStatCard("Solar today", `${latestSolarKwh.toFixed(2)} kWh`, "Resets at local midnight"),
    formatStatCard("Grid today", `${latestGridKwh.toFixed(2)} kWh`, "Resets at local midnight"),
    formatStatCard("Net export today", `${latestExportKwh.toFixed(2)} kWh`, "Positive solar minus grid since midnight"),
    formatStatCard("Estimated polls", `${imputedCount}`, "Readings imputed from the previous 3 samples")
  ].join("");
}

function renderChart(items) {
  const chartTheme = buildChartTheme();
  const dark = getTheme() === "dark";
  const bleGrid = items.filter((item) => item.grid_usage_watts !== null);
  const localSolar = items.filter((item) => item.solar_generation_watts !== null);
  const localGrid = items.filter((item) => item.source === "local_site" && item.grid_usage_watts !== null);
  const netRamp = computeRampSeries(buildNetItems(items), "net_power_watts");

  const traces = [
    {
      x: bleGrid.map((item) => item.observed_at),
      y: bleGrid.map((item) => item.grid_usage_watts),
      mode: "lines",
      name: "Consumption",
      line: { color: dark ? "#7fb0ff" : "#6f96d8", width: 1.5, shape: "linear" },
      fill: "tozeroy",
      fillcolor: dark ? "rgba(127, 176, 255, 0.16)" : "rgba(111, 150, 216, 0.17)"
    },
    {
      x: localGrid.map((item) => item.observed_at),
      y: localGrid.map((item) => item.grid_usage_watts),
      mode: "lines",
      name: "Site grid",
      line: { color: dark ? "#d98eff" : "#b57adf", width: 1.15, shape: "linear" },
      fill: "tozeroy",
      fillcolor: dark ? "rgba(217, 142, 255, 0.08)" : "rgba(181, 122, 223, 0.08)"
    },
    {
      x: localSolar.map((item) => item.observed_at),
      y: localSolar.map((item) => item.solar_generation_watts),
      mode: "lines",
      name: "Generation",
      line: { color: dark ? "#8ee29d" : "#7cc98a", width: 1.45, shape: "linear" },
      fill: "tozeroy",
      fillcolor: dark ? "rgba(142, 226, 157, 0.15)" : "rgba(124, 201, 138, 0.14)"
    },
    {
      x: netRamp.map((item) => item.observed_at),
      y: netRamp.map((item) => item.rate_kw_per_hr),
      mode: "lines",
      name: "Change rate",
      yaxis: "y2",
      line: {
        color: dark ? "#f08de0" : "#da78c6",
        width: 1.2,
        dash: "dot",
        shape: "linear"
      }
    }
  ];

  Plotly.react(chartElement, traces, {
    ...chartTheme,
    title: {
      text: "Generation / Consumption / Site Grid",
      font: { color: dark ? "#edf4ff" : "#263445", size: 16 }
    },
    yaxis: {
      ...chartTheme.yaxis,
      title: "Watts"
    },
    yaxis2: {
      title: "kW/hr",
      overlaying: "y",
      side: "right",
      tickfont: { color: dark ? "#97abc5" : "#7a8797", size: 11 },
      titlefont: { color: dark ? "#97abc5" : "#7a8797" },
      gridcolor: "rgba(0,0,0,0)",
      zeroline: false
    }
  }, {
    responsive: true,
    displaylogo: false,
    modeBarButtonsToRemove: ["lasso2d", "select2d"]
  });
}

function renderNetChart(items) {
  const chartTheme = buildChartTheme();
  const dark = getTheme() === "dark";
  const netItems = buildNetItems(items);
  const netRamp = computeRampSeries(netItems, "net_power_watts");

  Plotly.react(netChartElement, [
    {
      x: netItems.map((item) => item.observed_at),
      y: netItems.map((item) => item.net_power_watts),
      mode: "lines",
      name: "Export balance",
      line: { color: dark ? "#f08de0" : "#da78c6", width: 1.4, shape: "linear" },
      fill: "tozeroy",
      fillcolor: dark ? "rgba(240, 141, 224, 0.18)" : "rgba(218, 120, 198, 0.16)"
    },
    {
      x: netRamp.map((item) => item.observed_at),
      y: netRamp.map((item) => item.rate_w_per_min),
      mode: "lines",
      name: "Rate (W/min)",
      yaxis: "y2",
      line: {
        color: dark ? "#8ee29d" : "#7cc98a",
        width: 1.15,
        dash: "dot",
        shape: "linear"
      }
    },
    {
      x: netRamp.map((item) => item.observed_at),
      y: netRamp.map((item) => item.rate_kw_per_hr),
      mode: "lines",
      name: "Rate (kW/hr)",
      yaxis: "y3",
      line: {
        color: dark ? "#7fb0ff" : "#6f96d8",
        width: 1.1,
        dash: "dash",
        shape: "linear"
      }
    }
  ], {
    ...chartTheme,
    title: {
      text: "Export Balance",
      font: { color: dark ? "#edf4ff" : "#263445", size: 16 }
    },
    yaxis: {
      ...chartTheme.yaxis,
      title: "Solar - grid (W)",
      zeroline: true,
      zerolinecolor: dark ? "rgba(124, 147, 180, 0.42)" : "rgba(164, 179, 201, 0.42)"
    },
    yaxis2: {
      title: "W/min",
      overlaying: "y",
      side: "right",
      tickfont: { color: dark ? "#97abc5" : "#7a8797", size: 11 },
      titlefont: { color: dark ? "#97abc5" : "#7a8797" },
      gridcolor: "rgba(0,0,0,0)",
      zeroline: false
    },
    yaxis3: {
      title: "kW/hr",
      overlaying: "y",
      side: "right",
      anchor: "free",
      position: 0.94,
      tickfont: { color: dark ? "#97abc5" : "#7a8797", size: 11 },
      titlefont: { color: dark ? "#97abc5" : "#7a8797" },
      gridcolor: "rgba(0,0,0,0)",
      zeroline: false
    }
  }, {
    responsive: true,
    displaylogo: false,
    modeBarButtonsToRemove: ["lasso2d", "select2d"]
  });
}

function renderCumulativeChart(items) {
  const chartTheme = buildChartTheme();
  const dark = getTheme() === "dark";
  const solarItems = items
    .filter((item) => item.source === "local_site" && item.solar_generation_watts !== null)
    .sort((left, right) => new Date(left.observed_at) - new Date(right.observed_at));
  const bleGridItems = items
    .filter((item) => item.source === "ble" && item.grid_usage_watts !== null)
    .sort((left, right) => new Date(left.observed_at) - new Date(right.observed_at));
  const netItems = buildNetItems(items);

  const solarKwh = integrateSeriesKwh(solarItems, "solar_generation_watts");
  const gridKwh = integrateSeriesKwh(bleGridItems, "grid_usage_watts");
  const exportKwh = integrateSeriesKwh(
    netItems.map((item) => ({
      observed_at: item.observed_at,
      net_power_positive: Math.max(item.net_power_watts, 0)
    })),
    "net_power_positive"
  );

  Plotly.react(cumulativeChartElement, [
    {
      x: solarKwh.map((item) => item.observed_at),
      y: solarKwh.map((item) => item.cumulative_kwh),
      mode: "lines",
      name: "Solar cumulative",
      line: { color: dark ? "#8ee29d" : "#7cc98a", width: 1.6 },
      fill: "tozeroy",
      fillcolor: dark ? "rgba(142, 226, 157, 0.15)" : "rgba(124, 201, 138, 0.12)"
    },
    {
      x: gridKwh.map((item) => item.observed_at),
      y: gridKwh.map((item) => item.cumulative_kwh),
      mode: "lines",
      name: "Grid cumulative",
      line: { color: dark ? "#7fb0ff" : "#6f96d8", width: 1.5 },
      fill: "tozeroy",
      fillcolor: dark ? "rgba(127, 176, 255, 0.12)" : "rgba(111, 150, 216, 0.08)"
    },
    {
      x: exportKwh.map((item) => item.observed_at),
      y: exportKwh.map((item) => item.cumulative_kwh),
      mode: "lines",
      name: "Export cumulative",
      line: { color: dark ? "#f08de0" : "#da78c6", width: 1.4 },
      fill: "tozeroy",
      fillcolor: dark ? "rgba(240, 141, 224, 0.12)" : "rgba(218, 120, 198, 0.1)"
    }
  ], {
    ...chartTheme,
    title: {
      text: "Cumulative energy",
      font: { color: dark ? "#edf4ff" : "#263445", size: 16 }
    },
    yaxis: {
      ...chartTheme.yaxis,
      title: "kWh",
      rangemode: "tozero"
    }
  }, {
    responsive: true,
    displaylogo: false,
    modeBarButtonsToRemove: ["lasso2d", "select2d"]
  });
}

async function refresh() {
  const hours = Number(hoursInput.value || window.SOLAR_MONITOR_CONFIG.defaultHours || 24);
  const selectedDate = startDateInput.value;
  const selectedTime = startTimeInput.value || "00:00";
  const end = buildLocalDateTime(selectedDate, selectedTime);
  const start = new Date(end.getTime() - hours * 3600000);
  const [statusResponse, samplesResponse] = await Promise.all([
    fetch("/api/status"),
    fetch(`/api/samples?hours=${hours}&start=${encodeURIComponent(start.toISOString())}&end=${encodeURIComponent(end.toISOString())}`)
  ]);

  if (!statusResponse.ok || !samplesResponse.ok) {
    refreshText.textContent = "Refresh failed";
    return;
  }

  const statusPayload = await statusResponse.json();
  const samplesPayload = await samplesResponse.json();

  statusCards.innerHTML = statusPayload.pollers.map(formatStatusCard).join("");
  latestValues.innerHTML = statusPayload.latest_samples.map(formatMetricCard).join("");
  renderCumulativeStats(samplesPayload.items);
  renderChart(samplesPayload.items);
  renderNetChart(samplesPayload.items);
  renderCumulativeChart(samplesPayload.items);
  refreshText.textContent = `Updated ${new Date().toLocaleTimeString()}`;
}

const storedTheme = localStorage.getItem("solar-monitor-theme");
setTheme(storedTheme || "light");
if (!startDateInput.value) {
  const now = new Date();
  startDateInput.value = now.toISOString().slice(0, 10);
  startTimeInput.value = now.toTimeString().slice(0, 5);
}
themeToggle.addEventListener("click", () => {
  setTheme(getTheme() === "dark" ? "light" : "dark");
  refresh();
});
windowSlider.addEventListener("input", () => {
  hoursInput.value = windowSlider.value;
  refresh();
});
hoursInput.addEventListener("input", () => {
  windowSlider.value = hoursInput.value;
  refresh();
});
startDateInput.addEventListener("change", refresh);
startTimeInput.addEventListener("change", refresh);
startDateInput.addEventListener("input", refresh);
startTimeInput.addEventListener("input", refresh);
refresh();
setInterval(refresh, 10000);

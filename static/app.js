const hoursSelect = document.querySelector("#hours");
const statusCards = document.querySelector("#status-cards");
const latestValues = document.querySelector("#latest-values");
const refreshText = document.querySelector("#last-refresh");
const chartElement = document.querySelector("#chart");
const netChartElement = document.querySelector("#net-chart");

const chartTheme = {
  paper_bgcolor: "rgba(0,0,0,0)",
  plot_bgcolor: "#0e1a2b",
  margin: { t: 18, r: 18, b: 44, l: 52 },
  hovermode: "x unified",
  hoverlabel: {
    bgcolor: "rgba(9, 17, 28, 0.96)",
    bordercolor: "rgba(135, 156, 186, 0.22)",
    font: { color: "#edf4ff", size: 12 }
  },
  xaxis: {
    title: "",
    gridcolor: "rgba(124, 147, 180, 0.12)",
    tickfont: { color: "#97abc5", size: 11 },
    zeroline: false,
    linecolor: "rgba(124, 147, 180, 0.18)"
  },
  yaxis: {
    title: "",
    gridcolor: "rgba(124, 147, 180, 0.14)",
    tickfont: { color: "#97abc5", size: 11 },
    zeroline: false,
    rangemode: "tozero"
  },
  legend: {
    orientation: "h",
    y: -0.14,
    x: 0,
    font: { color: "#c0d0e5", size: 12 },
    bgcolor: "rgba(0,0,0,0)"
  }
};

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
    <article class="status-card">
      <strong>${item.name}</strong>
      <span class="status-pill status-${item.state}">${item.state}</span>
      <small><strong>last success</strong>: ${item.last_success_at ?? "never"}</small>
      ${errorBlock}
      ${details}
    </article>
  `;
}

function formatMetricCard(item) {
  return `
    <article class="metric-card">
      <span>${item.source}</span>
      <strong>Grid: ${formatNumber(item.grid_usage_watts)}</strong>
      <strong>Solar: ${formatNumber(item.solar_generation_watts)}</strong>
      <small>${item.observed_at}</small>
    </article>
  `;
}

function renderChart(items) {
  const bleGrid = items.filter((item) => item.grid_usage_watts !== null);
  const localSolar = items.filter((item) => item.solar_generation_watts !== null);
  const localGrid = items.filter((item) => item.source === "local_site" && item.grid_usage_watts !== null);

  const traces = [
    {
      x: bleGrid.map((item) => item.observed_at),
      y: bleGrid.map((item) => item.grid_usage_watts),
      mode: "lines",
      name: "Consumption",
      line: { color: "#7fb0ff", width: 1.5, shape: "linear" },
      fill: "tozeroy",
      fillcolor: "rgba(127, 176, 255, 0.16)"
    },
    {
      x: localGrid.map((item) => item.observed_at),
      y: localGrid.map((item) => item.grid_usage_watts),
      mode: "lines",
      name: "Site grid",
      line: { color: "#d98eff", width: 1.15, shape: "linear" },
      fill: "tozeroy",
      fillcolor: "rgba(217, 142, 255, 0.08)"
    },
    {
      x: localSolar.map((item) => item.observed_at),
      y: localSolar.map((item) => item.solar_generation_watts),
      mode: "lines",
      name: "Generation",
      line: { color: "#8ee29d", width: 1.45, shape: "linear" },
      fill: "tozeroy",
      fillcolor: "rgba(142, 226, 157, 0.15)"
    }
  ];

  Plotly.react(chartElement, traces, {
    ...chartTheme,
    title: {
      text: "Generation / Consumption / Site Grid",
      font: { color: "#edf4ff", size: 16 }
    },
    yaxis: {
      ...chartTheme.yaxis,
      title: "Watts"
    }
  }, {
    responsive: true,
    displaylogo: false,
    modeBarButtonsToRemove: ["lasso2d", "select2d"]
  });
}

function renderNetChart(items) {
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
      net_power_watts: Number(solarItem.solar_generation_watts) - Number(gridItem.grid_usage_watts)
    });
  }

  Plotly.react(netChartElement, [
    {
      x: netItems.map((item) => item.observed_at),
      y: netItems.map((item) => item.net_power_watts),
      mode: "lines",
      name: "Export balance",
      line: { color: "#f08de0", width: 1.4, shape: "linear" },
      fill: "tozeroy",
      fillcolor: "rgba(240, 141, 224, 0.18)"
    }
  ], {
    ...chartTheme,
    title: {
      text: "Export Balance",
      font: { color: "#edf4ff", size: 16 }
    },
    yaxis: {
      ...chartTheme.yaxis,
      title: "Solar - grid (W)",
      zeroline: true,
      zerolinecolor: "rgba(124, 147, 180, 0.42)"
    }
  }, {
    responsive: true,
    displaylogo: false,
    modeBarButtonsToRemove: ["lasso2d", "select2d"]
  });
}

async function refresh() {
  const hours = Number(hoursSelect.value || window.SOLAR_MONITOR_CONFIG.defaultHours || 24);
  const [statusResponse, samplesResponse] = await Promise.all([
    fetch("/api/status"),
    fetch(`/api/samples?hours=${hours}`)
  ]);

  if (!statusResponse.ok || !samplesResponse.ok) {
    refreshText.textContent = "Refresh failed";
    return;
  }

  const statusPayload = await statusResponse.json();
  const samplesPayload = await samplesResponse.json();

  statusCards.innerHTML = statusPayload.pollers.map(formatStatusCard).join("");
  latestValues.innerHTML = statusPayload.latest_samples.map(formatMetricCard).join("");
  renderChart(samplesPayload.items);
  renderNetChart(samplesPayload.items);
  refreshText.textContent = `Updated ${new Date().toLocaleTimeString()}`;
}

hoursSelect.addEventListener("change", refresh);
refresh();
setInterval(refresh, 10000);

const hoursSelect = document.querySelector("#hours");
const statusCards = document.querySelector("#status-cards");
const latestValues = document.querySelector("#latest-values");
const refreshText = document.querySelector("#last-refresh");
const chartElement = document.querySelector("#chart");

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
      mode: "lines+markers",
      name: "Grid usage (BLE)",
      marker: { color: "#d76b2a", size: 7 },
      line: { color: "#d76b2a", width: 2 }
    },
    {
      x: localGrid.map((item) => item.observed_at),
      y: localGrid.map((item) => item.grid_usage_watts),
      mode: "lines+markers",
      name: "Grid usage (local site)",
      marker: { color: "#7e5bef", size: 7 },
      line: { color: "#7e5bef", width: 2 }
    },
    {
      x: localSolar.map((item) => item.observed_at),
      y: localSolar.map((item) => item.solar_generation_watts),
      mode: "lines+markers",
      name: "Solar generation",
      marker: { color: "#2f8f6b", size: 7 },
      line: { color: "#2f8f6b", width: 2 }
    }
  ];

  Plotly.react(chartElement, traces, {
    paper_bgcolor: "rgba(0,0,0,0)",
    plot_bgcolor: "rgba(255, 248, 239, 0.65)",
    margin: { t: 10, r: 10, b: 50, l: 60 },
    xaxis: { title: "Time", gridcolor: "rgba(31, 42, 31, 0.08)" },
    yaxis: { title: "Watts", gridcolor: "rgba(31, 42, 31, 0.08)" },
    legend: { orientation: "h", y: 1.14 }
  }, { responsive: true });
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
  refreshText.textContent = `Updated ${new Date().toLocaleTimeString()}`;
}

hoursSelect.addEventListener("change", refresh);
refresh();
setInterval(refresh, 10000);

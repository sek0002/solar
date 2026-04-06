const hoursInput = document.querySelector("#hours");
const windowPreset = document.querySelector("#window-preset");
const startDateInput = document.querySelector("#start-date");
const startTimeInput = document.querySelector("#start-time");
const statusCards = document.querySelector("#status-cards");
const latestValues = document.querySelector("#latest-values");
const cumulativeStats = document.querySelector("#cumulative-stats");
const totalsTableBody = document.querySelector("#totals-table-body");
const refreshText = document.querySelector("#last-refresh");
const themeToggle = document.querySelector("#theme-toggle");
const bleChartElement = document.querySelector("#ble-chart");
const solarChartElement = document.querySelector("#solar-chart");
const netChartElement = document.querySelector("#net-chart");
const cumulativeChartElement = document.querySelector("#cumulative-chart");
const hourlyChartElement = document.querySelector("#hourly-chart");
const dailyChartElement = document.querySelector("#daily-chart");
const appTimezone = window.SOLAR_MONITOR_CONFIG.timezoneName || "Australia/Melbourne";
const uiStateKey = "solar-monitor-ui-state";
const charts = {};

if (window["chartjs-plugin-annotation"]) {
  Chart.register(window["chartjs-plugin-annotation"]);
}

function loadUiState() {
  try {
    return JSON.parse(localStorage.getItem(uiStateKey) || "{}");
  } catch (error) {
    console.warn("Unable to parse stored UI state", error);
    return {};
  }
}

let uiState = loadUiState();

function saveUiState() {
  localStorage.setItem(uiStateKey, JSON.stringify(uiState));
}

function updateUiState(mutator) {
  mutator(uiState);
  saveUiState();
}

function getZonedParts(dateLike) {
  const formatter = new Intl.DateTimeFormat("en-AU", {
    timeZone: appTimezone,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false
  });
  return Object.fromEntries(
    formatter.formatToParts(new Date(dateLike)).map((part) => [part.type, part.value])
  );
}

function getTheme() {
  return document.documentElement.dataset.theme || "light";
}

function setTheme(theme) {
  document.documentElement.dataset.theme = theme;
  localStorage.setItem("solar-monitor-theme", theme);
  themeToggle.textContent = theme === "dark" ? "Light mode" : "Dark mode";
}

function buildLocalDateTime(dateValue, timeValue = "00:00") {
  if (!dateValue) {
    return new Date();
  }
  const [year, month, day] = dateValue.split("-").map(Number);
  const [hours, minutes] = (timeValue || "00:00").split(":").map(Number);
  return new Date(year, (month || 1) - 1, day || 1, hours || 0, minutes || 0, 0, 0);
}

function formatLocalDate(date) {
  const parts = getZonedParts(date);
  return `${parts.year}-${parts.month}-${parts.day}`;
}

function clampHours(value) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return Number(window.SOLAR_MONITOR_CONFIG.defaultHours || 24);
  }
  return Math.min(168, Math.max(1, Math.round(numeric)));
}

function syncWindowControls(hours) {
  const clampedHours = clampHours(hours);
  hoursInput.value = clampedHours;
  const presetValues = Array.from(windowPreset.options).map((option) => option.value);
  windowPreset.value = presetValues.includes(String(clampedHours)) ? String(clampedHours) : "custom";
  updateUiState((state) => {
    state.controls = state.controls || {};
    state.controls.hours = clampedHours;
  });
  return clampedHours;
}

function ensureStartInputs() {
  if (startDateInput.value) {
    if (!startTimeInput.value) {
      startTimeInput.value = "00:00";
    }
    return;
  }
  const now = new Date();
  const start = new Date(now.getTime() - clampHours(hoursInput.value || window.SOLAR_MONITOR_CONFIG.defaultHours || 24) * 3600000);
  startDateInput.value = formatLocalDate(start);
  startTimeInput.value = `${String(start.getHours()).padStart(2, "0")}:${String(start.getMinutes()).padStart(2, "0")}`;
}

function persistDateTimeControls() {
  updateUiState((state) => {
    state.controls = state.controls || {};
    state.controls.startDate = startDateInput.value || "";
    state.controls.startTime = startTimeInput.value || "";
  });
}

function chartTheme() {
  const dark = getTheme() === "dark";
  return {
    text: dark ? "#edf4ff" : "#263445",
    muted: dark ? "#97abc5" : "#7a8797",
    grid: dark ? "rgba(124, 147, 180, 0.14)" : "rgba(164, 179, 201, 0.16)",
    panel: dark ? "#132134" : "#ffffff",
    blue: dark ? "#7fb0ff" : "#6f96d8",
    green: dark ? "#8ee29d" : "#7cc98a",
    pink: dark ? "#f08de0" : "#da78c6",
    red: dark ? "#ff6b6b" : "#d62828",
    tooltipBg: dark ? "rgba(13, 22, 35, 0.96)" : "rgba(255,255,255,0.95)"
  };
}

function formatWMin(value) {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "n/a";
  }
  return `${Number(value).toFixed(1)} W/min`;
}

function formatKwPerHr(value) {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "n/a";
  }
  return `${ratePerMinuteToKwPerHour(value).toFixed(3)} kW/hr`;
}

function ratePerMinuteToKwh(ratePerMinute, deltaMinutes) {
  return (Number(ratePerMinute) * deltaMinutes) / 60000;
}

function ratePerMinuteToKwPerHour(value) {
  return (Number(value) * 60) / 1000;
}

function getDayKey(dateLike) {
  const parts = getZonedParts(dateLike);
  return `${parts.year}-${parts.month}-${parts.day}`;
}

function getHourKey(dateLike) {
  const parts = getZonedParts(dateLike);
  return `${parts.year}-${parts.month}-${parts.day} ${parts.hour}:00`;
}

function formatDateTime(dateLike) {
  if (!dateLike) {
    return "never";
  }
  return new Date(dateLike).toLocaleString("en-AU", { timeZone: appTimezone });
}

function formatChartLabel(dateLike) {
  return new Date(dateLike).toLocaleString("en-AU", {
    timeZone: appTimezone,
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false
  });
}

function formatAxisLabel(dateLike) {
  return new Date(dateLike).toLocaleString("en-AU", {
    timeZone: appTimezone,
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false
  });
}

function minuteOfDay(dateLike) {
  const parts = getZonedParts(dateLike);
  return (Number(parts.hour) * 60) + Number(parts.minute);
}

function getAreaFillColor(color) {
  const fills = {
    "#6f96d8": "rgba(111, 150, 216, 0.16)",
    "#7fb0ff": "rgba(127, 176, 255, 0.16)",
    "#7cc98a": "rgba(124, 201, 138, 0.16)",
    "#8ee29d": "rgba(142, 226, 157, 0.16)",
    "#da78c6": "rgba(218, 120, 198, 0.16)",
    "#f08de0": "rgba(240, 141, 224, 0.16)"
  };
  return fills[color] || "rgba(120, 140, 170, 0.16)";
}

function formatStatusCard(item) {
  const details = Object.entries(item.details || {})
    .map(([key, value]) => {
      const displayValue = key.endsWith("_at") && value ? formatDateTime(value) : (value === null || value === undefined ? "n/a" : value);
      return `<small><strong>${key}</strong>: ${displayValue}</small>`;
    })
    .join("");
  const errorBlock = item.last_error ? `<small><strong>last error</strong>: ${item.last_error}</small>` : "";
  return `
    <details class="status-card" data-status-name="${item.name}">
      <summary>
        <strong>${item.name}</strong>
        <span class="status-pill status-${item.state}">${item.state}</span>
      </summary>
      <div class="status-minimal">
        <small>${formatDateTime(item.last_success_at)}</small>
        <small>${item.last_error ? "Issue recorded" : "Healthy"}</small>
      </div>
      <div class="status-card-body">
        <small><strong>last success</strong>: ${formatDateTime(item.last_success_at)}</small>
        ${errorBlock}
        ${details}
      </div>
    </details>
  `;
}

function getExpandedStatusNames() {
  if (Array.isArray(uiState.expandedStatuses) && uiState.expandedStatuses.length) {
    return new Set(uiState.expandedStatuses);
  }
  return new Set(
    Array.from(statusCards.querySelectorAll(".status-card[open]"))
      .map((element) => element.dataset.statusName)
      .filter(Boolean)
  );
}

function bindStatusCardPersistence(root = statusCards) {
  root.querySelectorAll(".status-card").forEach((element) => {
    if (element.dataset.toggleBound === "true") {
      return;
    }
    element.dataset.toggleBound = "true";
    element.addEventListener("toggle", () => {
      updateUiState((state) => {
        state.expandedStatuses = Array.from(statusCards.querySelectorAll(".status-card[open]"))
          .map((statusElement) => statusElement.dataset.statusName)
          .filter(Boolean);
      });
    });
  });
}

function renderStatusCards(items) {
  const expandedNames = getExpandedStatusNames();
  statusCards.innerHTML = items.map((item) => formatStatusCard(item)).join("");
  statusCards.querySelectorAll(".status-card").forEach((element) => {
    if (expandedNames.has(element.dataset.statusName)) {
      element.open = true;
    }
  });
  bindStatusCardPersistence();
}

function buildRateDisplay(label, value) {
  return `
    <div class="metric-reading">
      <span>${label}</span>
      <strong>${formatKwPerHr(value)}</strong>
      <small>${formatWMin(value)}</small>
    </div>
  `;
}

function formatMetricCard(item) {
  const isImputed = item.raw_payload && item.raw_payload.imputed;
  return `
    <article class="metric-card">
      <span>${item.source}</span>
      ${buildRateDisplay("Grid", item.grid_usage_watts)}
      ${buildRateDisplay("Solar", item.solar_generation_watts)}
      <small>${isImputed ? "Estimated from previous readings" : "Live reading"}</small>
      <small>${formatDateTime(item.observed_at)}</small>
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
    if (new Date(gridItem.observed_at).getTime() > solarTime) {
      continue;
    }
    netItems.push({
      observed_at: solarItem.observed_at,
      net_power_watts: Number(solarItem.solar_generation_watts) - Number(gridItem.grid_usage_watts)
    });
  }
  return netItems;
}

function limitRecentSeries(series, count = 6) {
  return series.slice(Math.max(0, series.length - count));
}

function computeLinearTrend(series, valueKey) {
  const recent = limitRecentSeries(series.filter((item) => item[valueKey] !== null), 6);
  if (recent.length < 2) {
    return null;
  }
  const baseTime = new Date(recent[0].observed_at).getTime();
  const points = recent.map((item) => ({
    x: (new Date(item.observed_at).getTime() - baseTime) / 60000,
    y: Number(item[valueKey])
  }));
  const n = points.length;
  const sumX = points.reduce((accumulator, point) => accumulator + point.x, 0);
  const sumY = points.reduce((accumulator, point) => accumulator + point.y, 0);
  const sumXY = points.reduce((accumulator, point) => accumulator + (point.x * point.y), 0);
  const sumXX = points.reduce((accumulator, point) => accumulator + (point.x * point.x), 0);
  const denominator = (n * sumXX) - (sumX * sumX);
  const slope = denominator === 0 ? 0 : ((n * sumXY) - (sumX * sumY)) / denominator;
  const intercept = (sumY - (slope * sumX)) / n;
  return { baseTime, slope, intercept, latestObservedAt: recent[recent.length - 1].observed_at };
}

function buildProjectionSeries(series, valueKey, minutesAhead = 60, stepMinutes = 5) {
  const trend = computeLinearTrend(series, valueKey);
  if (!trend) {
    return [];
  }
  const lastObserved = new Date(trend.latestObservedAt).getTime();
  const points = [];
  for (let offset = stepMinutes; offset <= minutesAhead; offset += stepMinutes) {
    const observedAt = new Date(lastObserved + (offset * 60000));
    const x = (observedAt.getTime() - trend.baseTime) / 60000;
    points.push({
      observed_at: observedAt.toISOString(),
      projected_value: Math.max(0, trend.intercept + (trend.slope * x))
    });
  }
  return points;
}

function buildWeeklyAverageSeries(historySeries, valueKey, anchorSeries, minutesAhead = 60, stepMinutes = 5, toleranceMinutes = 20) {
  const usableHistory = historySeries.filter((item) => item[valueKey] !== null);
  const anchor = anchorSeries.length ? new Date(anchorSeries[anchorSeries.length - 1].observed_at) : new Date();
  if (!usableHistory.length) {
    return [];
  }
  const points = [];
  for (let offset = stepMinutes; offset <= minutesAhead; offset += stepMinutes) {
    const sampleTime = new Date(anchor.getTime() + (offset * 60000));
    const targetMinute = minuteOfDay(sampleTime);
    const matching = usableHistory.filter((item) => {
      const diff = Math.abs(minuteOfDay(item.observed_at) - targetMinute);
      return Math.min(diff, 1440 - diff) <= toleranceMinutes;
    });
    if (!matching.length) {
      continue;
    }
    points.push({
      observed_at: sampleTime.toISOString(),
      average_value: matching.reduce((accumulator, item) => accumulator + Number(item[valueKey]), 0) / matching.length
    });
  }
  return points;
}

function integrateSeriesKwh(series, valueKey) {
  let cumulative = 0;
  const points = [];
  for (let index = 0; index < series.length; index += 1) {
    const current = series[index];
    if (index > 0) {
      const previous = series[index - 1];
      const deltaMinutes = (new Date(current.observed_at) - new Date(previous.observed_at)) / 60000;
      const averageRate = (Number(previous[valueKey]) + Number(current[valueKey])) / 2;
      if (deltaMinutes > 0) {
        cumulative += ratePerMinuteToKwh(averageRate, deltaMinutes);
      }
    }
    points.push({ observed_at: current.observed_at, cumulative_kwh: cumulative });
  }
  return points;
}

function aggregateSeriesEnergy(series, valueKey, keyFn) {
  const totals = new Map();
  for (let index = 1; index < series.length; index += 1) {
    const previous = series[index - 1];
    const current = series[index];
    const deltaMinutes = (new Date(current.observed_at) - new Date(previous.observed_at)) / 60000;
    if (deltaMinutes <= 0) {
      continue;
    }
    const averageRate = (Number(previous[valueKey]) + Number(current[valueKey])) / 2;
    const bucketKey = keyFn(current.observed_at);
    totals.set(bucketKey, (totals.get(bucketKey) || 0) + ratePerMinuteToKwh(averageRate, deltaMinutes));
  }
  return totals;
}

function combineEnergyBuckets(solarSeries, bleSeries, keyFn) {
  const solarTotals = aggregateSeriesEnergy(solarSeries, "solar_generation_watts", keyFn);
  const gridTotals = aggregateSeriesEnergy(bleSeries, "grid_usage_watts", keyFn);
  const keys = Array.from(new Set([...solarTotals.keys(), ...gridTotals.keys()])).sort();
  return keys.map((key) => ({
    key,
    solar_kwh: solarTotals.get(key) || 0,
    grid_kwh: gridTotals.get(key) || 0,
    net_kwh: (solarTotals.get(key) || 0) - (gridTotals.get(key) || 0)
  }));
}

function startOfToday() {
  const now = new Date();
  const parts = getZonedParts(now);
  return new Date(Number(parts.year), Number(parts.month) - 1, Number(parts.day), 0, 0, 0, 0);
}

function startOfWeek() {
  return new Date(Date.now() - 7 * 24 * 3600000);
}

function computePeriodTotals(series, valueKey, sinceDate) {
  const filtered = series.filter((item) => new Date(item.observed_at) >= sinceDate);
  const cumulative = integrateSeriesKwh(filtered, valueKey);
  return cumulative.length ? cumulative[cumulative.length - 1].cumulative_kwh : 0;
}

function getBleBatteryPercent(pollers) {
  const blePoller = (pollers || []).find((item) => item.name === "ble");
  const batteryPercent = blePoller && blePoller.details ? blePoller.details.battery_percent : null;
  return Number.isFinite(Number(batteryPercent)) ? Number(batteryPercent) : null;
}

function renderTotals(items, pollers = []) {
  const solarItems = items
    .filter((item) => item.source === "local_site" && item.solar_generation_watts !== null)
    .sort((left, right) => new Date(left.observed_at) - new Date(right.observed_at));
  const bleGridItems = items
    .filter((item) => item.source === "ble" && item.grid_usage_watts !== null)
    .sort((left, right) => new Date(left.observed_at) - new Date(right.observed_at));
  const dayStart = startOfToday();
  const weekStart = startOfWeek();

  const dailySolar = computePeriodTotals(solarItems, "solar_generation_watts", dayStart);
  const dailyGrid = computePeriodTotals(bleGridItems, "grid_usage_watts", dayStart);
  const weeklySolar = computePeriodTotals(solarItems, "solar_generation_watts", weekStart);
  const weeklyGrid = computePeriodTotals(bleGridItems, "grid_usage_watts", weekStart);
  const batteryPercent = getBleBatteryPercent(pollers);
  const imputedCount = items.filter((item) => item.raw_payload && item.raw_payload.imputed).length;

  cumulativeStats.innerHTML = [
    formatStatCard("Daily net", `${(dailySolar - dailyGrid).toFixed(2)} kWh`, "Solar minus BLE grid"),
    formatStatCard("Weekly net", `${(weeklySolar - weeklyGrid).toFixed(2)} kWh`, "Solar minus BLE grid"),
    formatStatCard("Powerpal battery", batteryPercent === null ? "n/a" : `${batteryPercent}%`, "Latest BLE battery reading"),
    formatStatCard("Estimated polls", `${imputedCount}`, "Readings imputed from previous samples")
  ].join("");

  totalsTableBody.innerHTML = [
    { label: "Daily", solar: dailySolar, grid: dailyGrid, net: dailySolar - dailyGrid },
    { label: "Weekly", solar: weeklySolar, grid: weeklyGrid, net: weeklySolar - weeklyGrid }
  ].map((row) => `
    <tr>
      <td>${row.label}</td>
      <td>${row.solar.toFixed(2)} kWh</td>
      <td>${row.grid.toFixed(2)} kWh</td>
      <td>${row.net.toFixed(2)} kWh</td>
    </tr>
  `).join("");
}

function destroyChart(chartKey) {
  if (charts[chartKey]) {
    charts[chartKey].destroy();
    delete charts[chartKey];
  }
}

function buildBaseChartOptions(kind = "line", withSecondaryAxis = false) {
  const theme = chartTheme();
  return {
    responsive: true,
    maintainAspectRatio: false,
    interaction: {
      mode: "index",
      intersect: false
    },
    animation: false,
    plugins: {
      legend: {
        labels: {
          color: theme.muted,
          usePointStyle: true,
          boxWidth: 10
        }
      },
      tooltip: {
        backgroundColor: theme.tooltipBg,
        titleColor: theme.text,
        bodyColor: theme.text,
        borderColor: theme.grid,
        borderWidth: 1,
        callbacks: {
          title(items) {
            return items.length ? formatDateTime(items[0].label) : "";
          }
        }
      },
      annotation: {
        annotations: {}
      }
    },
    scales: {
      x: {
        ticks: {
          color: theme.muted,
          maxRotation: 0,
          autoSkip: true,
          maxTicksLimit: kind === "bar" ? 10 : 8,
          callback(value) {
            return formatAxisLabel(this.getLabelForValue(value));
          }
        },
        grid: {
          color: theme.grid
        }
      },
      y: {
        title: {
          display: true,
          text: kind === "bar" ? "kWh" : "kW/hr",
          color: theme.muted
        },
        ticks: {
          color: theme.muted
        },
        grid: {
          color: theme.grid
        },
        beginAtZero: true
      }
    }
  };
}

function buildNowAnnotation(labels) {
  if (!labels.length) {
    return {};
  }
  const now = Date.now();
  const currentLabel = labels.reduce((closest, label) => {
    if (!closest) {
      return label;
    }
    const closestDiff = Math.abs(new Date(closest).getTime() - now);
    const labelDiff = Math.abs(new Date(label).getTime() - now);
    return labelDiff < closestDiff ? label : closest;
  }, null);
  return {
    currentTime: {
      type: "line",
      scaleID: "x",
      value: currentLabel,
      borderColor: chartTheme().red,
      borderWidth: 2,
      borderDash: [6, 4]
    }
  };
}

function createOrUpdateChart(chartKey, canvas, config) {
  destroyChart(chartKey);
  charts[chartKey] = new Chart(canvas.getContext("2d"), config);
}

function alignSeriesDatasets(seriesGroups, valueAccessor) {
  const labels = Array.from(new Set(
    seriesGroups.flatMap((group) => group.points.map((point) => new Date(point.observed_at).toISOString()))
  ));
  labels.sort((left, right) => new Date(left) - new Date(right));
  return {
    labels,
    datasets: seriesGroups.map((group) => {
      const byLabel = new Map(group.points.map((point) => [new Date(point.observed_at).toISOString(), point]));
      const rawRates = [];
      const data = labels.map((label) => {
        const point = byLabel.get(label);
        if (!point) {
          rawRates.push(null);
          return null;
        }
        rawRates.push(valueAccessor(point, "wmin"));
        return valueAccessor(point, "kwhr");
      });
      return { ...group.dataset, data, rawRates };
    })
  };
}

function buildRateTooltipCallbacks() {
  return {
    label(context) {
      const rawRate = context.dataset.rawRates ? context.dataset.rawRates[context.dataIndex] : null;
      if (context.raw === null || context.raw === undefined) {
        return `${context.dataset.label}: n/a`;
      }
      return [
        `${context.dataset.label}: ${Number(context.raw).toFixed(3)} kW/hr`,
        `${formatWMin(rawRate)}`
      ];
    }
  };
}

function renderRateChart(chartKey, canvas, title, series, valueKey, color, historySeries = []) {
  const theme = chartTheme();
  const forecastSeries = buildProjectionSeries(series, valueKey);
  const averageSeries = buildWeeklyAverageSeries(historySeries, valueKey, series);
  const aligned = alignSeriesDatasets([
    {
      points: series,
      dataset: {
        label: title,
        borderColor: color,
        backgroundColor: getAreaFillColor(color),
        tension: 0.2,
        fill: true,
        pointRadius: 0,
        pointHoverRadius: 3,
        borderWidth: 2
      }
    },
    {
      points: forecastSeries,
      dataset: {
        label: `${title} trend`,
        borderColor: color,
        tension: 0.2,
        fill: false,
        pointRadius: 0,
        pointHoverRadius: 3,
        borderWidth: 1.5,
        borderDash: [5, 5]
      }
    },
    {
      points: averageSeries,
      dataset: {
        label: `${title} weekly mean`,
        borderColor: color,
        tension: 0.2,
        fill: false,
        pointRadius: 0,
        pointHoverRadius: 3,
        borderWidth: 1.2,
        borderDash: [8, 4],
        opacity: 0.35
      }
    }
  ], (point, unit) => {
    const value = Number(
      point.projected_value !== undefined && point.projected_value !== null
        ? point.projected_value
        : (point.average_value !== undefined && point.average_value !== null
          ? point.average_value
          : point[valueKey])
    );
    return unit === "kwhr" ? ratePerMinuteToKwPerHour(value) : value;
  });

  const options = buildBaseChartOptions("line");
  options.plugins.title = {
    display: true,
    text: title,
    color: theme.text,
    font: { size: 16, weight: "600" }
  };
  options.plugins.tooltip.callbacks = buildRateTooltipCallbacks();
  options.plugins.annotation.annotations = buildNowAnnotation(aligned.labels);

  createOrUpdateChart(chartKey, canvas, {
    type: "line",
    data: aligned,
    options
  });
}

function renderNetChart(items, historyItems = []) {
  const theme = chartTheme();
  const netItems = buildNetItems(items);
  const historyNetItems = buildNetItems(historyItems);
  const forecastSeries = buildProjectionSeries(
    netItems.map((item) => ({ observed_at: item.observed_at, net_power_watts: item.net_power_watts })),
    "net_power_watts"
  );
  const averageSeries = buildWeeklyAverageSeries(
    historyNetItems.map((item) => ({ observed_at: item.observed_at, net_power_watts: item.net_power_watts })),
    "net_power_watts",
    netItems.map((item) => ({ observed_at: item.observed_at, net_power_watts: item.net_power_watts }))
  );
  const aligned = alignSeriesDatasets([
    {
      points: netItems,
      dataset: {
        label: "Net balance",
        borderColor: theme.pink,
        backgroundColor: getAreaFillColor(theme.pink),
        tension: 0.2,
        fill: true,
        pointRadius: 0,
        pointHoverRadius: 3,
        borderWidth: 2
      }
    },
    {
      points: forecastSeries,
      dataset: {
        label: "Net balance trend",
        borderColor: theme.pink,
        tension: 0.2,
        fill: false,
        pointRadius: 0,
        pointHoverRadius: 3,
        borderWidth: 1.5,
        borderDash: [5, 5]
      }
    },
    {
      points: averageSeries,
      dataset: {
        label: "Net balance weekly mean",
        borderColor: theme.pink,
        tension: 0.2,
        fill: false,
        pointRadius: 0,
        pointHoverRadius: 3,
        borderWidth: 1.2,
        borderDash: [8, 4]
      }
    }
  ], (point, unit) => {
    const value = Number(
      point.projected_value !== undefined && point.projected_value !== null
        ? point.projected_value
        : (point.average_value !== undefined && point.average_value !== null
          ? point.average_value
          : point.net_power_watts)
    );
    return unit === "kwhr" ? ratePerMinuteToKwPerHour(value) : value;
  });

  const options = buildBaseChartOptions("line");
  options.plugins.title = {
    display: true,
    text: "Net balance",
    color: theme.text,
    font: { size: 16, weight: "600" }
  };
  options.plugins.tooltip.callbacks = buildRateTooltipCallbacks();
  options.plugins.annotation.annotations = buildNowAnnotation(aligned.labels);

  createOrUpdateChart("net", netChartElement, {
    type: "line",
    data: aligned,
    options
  });
}

function renderCumulativeChart(items) {
  const theme = chartTheme();
  const solarItems = items
    .filter((item) => item.source === "local_site" && item.solar_generation_watts !== null)
    .sort((left, right) => new Date(left.observed_at) - new Date(right.observed_at));
  const bleGridItems = items
    .filter((item) => item.source === "ble" && item.grid_usage_watts !== null)
    .sort((left, right) => new Date(left.observed_at) - new Date(right.observed_at));
  const solarKwh = integrateSeriesKwh(solarItems, "solar_generation_watts");
  const gridKwh = integrateSeriesKwh(bleGridItems, "grid_usage_watts");
  const aligned = alignSeriesDatasets([
    {
      points: gridKwh,
      dataset: {
        label: "BLE grid cumulative",
        borderColor: theme.blue,
        backgroundColor: "transparent",
        tension: 0.2,
        fill: false,
        pointRadius: 0,
        pointHoverRadius: 3,
        borderWidth: 2
      }
    },
    {
      points: solarKwh,
      dataset: {
        label: "Site solar cumulative",
        borderColor: theme.green,
        backgroundColor: "transparent",
        tension: 0.2,
        fill: false,
        pointRadius: 0,
        pointHoverRadius: 3,
        borderWidth: 2
      }
    }
  ], (point) => point.cumulative_kwh);
  aligned.datasets.forEach((dataset) => {
    dataset.rawRates = dataset.data;
  });

  const options = buildBaseChartOptions("line");
  options.scales.y.title.text = "kWh";
  options.plugins.title = {
    display: true,
    text: "Cumulative energy",
    color: theme.text,
    font: { size: 16, weight: "600" }
  };
  options.plugins.tooltip.callbacks = {
    label(context) {
      return `${context.dataset.label}: ${Number(context.raw).toFixed(3)} kWh`;
    }
  };
  options.plugins.annotation.annotations = buildNowAnnotation(aligned.labels);

  createOrUpdateChart("cumulative", cumulativeChartElement, {
    type: "line",
    data: aligned,
    options
  });
}

function renderEnergyBars(chartKey, canvas, title, buckets) {
  const theme = chartTheme();
  const labels = buckets.map((item) => item.key);
  const options = buildBaseChartOptions("bar");
  options.plugins.title = {
    display: true,
    text: title,
    color: theme.text,
    font: { size: 16, weight: "600" }
  };
  options.plugins.tooltip.callbacks = {
    label(context) {
      return `${context.dataset.label}: ${Number(context.raw).toFixed(3)} kWh`;
    }
  };

  createOrUpdateChart(chartKey, canvas, {
    data: {
      labels,
      datasets: [
        {
          type: "bar",
          label: "BLE grid",
          data: buckets.map((item) => item.grid_kwh),
          backgroundColor: getAreaFillColor(theme.blue),
          borderColor: theme.blue,
          borderWidth: 1
        },
        {
          type: "bar",
          label: "Site solar",
          data: buckets.map((item) => item.solar_kwh),
          backgroundColor: getAreaFillColor(theme.green),
          borderColor: theme.green,
          borderWidth: 1
        },
        {
          type: "line",
          label: "Net",
          data: buckets.map((item) => item.net_kwh),
          borderColor: theme.pink,
          backgroundColor: "transparent",
          pointRadius: 2,
          pointHoverRadius: 4,
          tension: 0.2,
          borderWidth: 2
        }
      ]
    },
    options
  });
}

function renderEmptyCharts() {
  const emptyCharts = [
    ["ble", bleChartElement, "BLE grid"],
    ["solar", solarChartElement, "Site solar"],
    ["net", netChartElement, "Net balance"],
    ["cumulative", cumulativeChartElement, "Cumulative energy"],
    ["hourly", hourlyChartElement, "Hourly energy"],
    ["daily", dailyChartElement, "Daily energy"]
  ];
  emptyCharts.forEach(([chartKey, canvas, title]) => {
    const options = buildBaseChartOptions(chartKey === "hourly" || chartKey === "daily" ? "bar" : "line");
    options.plugins.title = {
      display: true,
      text: title,
      color: chartTheme().text,
      font: { size: 16, weight: "600" }
    };
    createOrUpdateChart(chartKey, canvas, {
      type: chartKey === "hourly" || chartKey === "daily" ? "bar" : "line",
      data: { labels: [], datasets: [] },
      options
    });
  });
}

async function refresh() {
  try {
    ensureStartInputs();
    const hours = syncWindowControls(hoursInput.value || window.SOLAR_MONITOR_CONFIG.defaultHours || 24);
    const selectedDate = startDateInput.value;
    const selectedTime = startTimeInput.value || "00:00";
    const start = buildLocalDateTime(selectedDate, selectedTime);
    const safeStart = Number.isNaN(start.getTime()) ? new Date() : start;
    const end = new Date(safeStart.getTime() + hours * 3600000);
    const historyEnd = new Date();
    const [statusResponse, samplesResponse, historyResponse] = await Promise.all([
      fetch("/api/status"),
      fetch(`/api/samples?hours=${hours}&start=${encodeURIComponent(safeStart.toISOString())}&end=${encodeURIComponent(end.toISOString())}`),
      fetch(`/api/samples?hours=168&end=${encodeURIComponent(historyEnd.toISOString())}`)
    ]);

    if (!statusResponse.ok || !samplesResponse.ok || !historyResponse.ok) {
      throw new Error(`HTTP ${statusResponse.status}/${samplesResponse.status}/${historyResponse.status}`);
    }

    const statusPayload = await statusResponse.json();
    const samplesPayload = await samplesResponse.json();
    const historyPayload = await historyResponse.json();
    const items = Array.isArray(samplesPayload.items) ? samplesPayload.items : [];
    const historyItems = Array.isArray(historyPayload.items) ? historyPayload.items : [];

    renderStatusCards(statusPayload.pollers);
    latestValues.innerHTML = statusPayload.latest_samples.map(formatMetricCard).join("");

    if (!items.length) {
      cumulativeStats.innerHTML = "";
      totalsTableBody.innerHTML = `
        <tr><td>Daily</td><td>0.00 kWh</td><td>0.00 kWh</td><td>0.00 kWh</td></tr>
        <tr><td>Weekly</td><td>0.00 kWh</td><td>0.00 kWh</td><td>0.00 kWh</td></tr>
      `;
      renderEmptyCharts();
      refreshText.textContent = "No data in selected window";
      return;
    }

    const bleItems = items
      .filter((item) => item.source === "ble" && item.grid_usage_watts !== null)
      .sort((left, right) => new Date(left.observed_at) - new Date(right.observed_at));
    const solarItems = items
      .filter((item) => item.source === "local_site" && item.solar_generation_watts !== null)
      .sort((left, right) => new Date(left.observed_at) - new Date(right.observed_at));
    const historyBleItems = historyItems
      .filter((item) => item.source === "ble" && item.grid_usage_watts !== null)
      .sort((left, right) => new Date(left.observed_at) - new Date(right.observed_at));
    const historySolarItems = historyItems
      .filter((item) => item.source === "local_site" && item.solar_generation_watts !== null)
      .sort((left, right) => new Date(left.observed_at) - new Date(right.observed_at));

    renderRateChart("ble", bleChartElement, "BLE grid", bleItems, "grid_usage_watts", chartTheme().blue, historyBleItems);
    renderRateChart("solar", solarChartElement, "Site solar", solarItems, "solar_generation_watts", chartTheme().green, historySolarItems);
    renderNetChart(items, historyItems);
    renderCumulativeChart(items);
    renderEnergyBars("hourly", hourlyChartElement, "Hourly energy", combineEnergyBuckets(solarItems, bleItems, getHourKey));
    renderEnergyBars("daily", dailyChartElement, "Daily energy", combineEnergyBuckets(solarItems, bleItems, getDayKey));
    renderTotals(items, statusPayload.pollers);

    refreshText.textContent = `Updated ${new Date().toLocaleTimeString("en-AU", { timeZone: appTimezone })}`;
  } catch (error) {
    console.error("Refresh failed", error);
    renderEmptyCharts();
    refreshText.textContent = "Refresh failed";
  }
}

let pendingRefresh = null;

function scheduleRefresh(delay = 150) {
  if (pendingRefresh) {
    window.clearTimeout(pendingRefresh);
  }
  pendingRefresh = window.setTimeout(() => {
    pendingRefresh = null;
    refresh();
  }, delay);
}

const storedTheme = localStorage.getItem("solar-monitor-theme");
setTheme(storedTheme || "light");
if (uiState.controls && uiState.controls.startDate) {
  startDateInput.value = uiState.controls.startDate;
}
if (uiState.controls && uiState.controls.startTime) {
  startTimeInput.value = uiState.controls.startTime;
}
ensureStartInputs();
syncWindowControls((uiState.controls && uiState.controls.hours) || hoursInput.value || window.SOLAR_MONITOR_CONFIG.defaultHours || 24);
persistDateTimeControls();
Array.from(statusCards.querySelectorAll(".status-card")).forEach((element) => {
  if (getExpandedStatusNames().has(element.dataset.statusName)) {
    element.open = true;
  }
});
bindStatusCardPersistence();
themeToggle.addEventListener("click", () => {
  setTheme(getTheme() === "dark" ? "light" : "dark");
  refresh();
});
windowPreset.addEventListener("change", () => {
  if (windowPreset.value !== "custom") {
    syncWindowControls(windowPreset.value);
    scheduleRefresh(0);
  }
});
hoursInput.addEventListener("input", () => {
  syncWindowControls(hoursInput.value);
  scheduleRefresh();
});
hoursInput.addEventListener("change", () => {
  syncWindowControls(hoursInput.value);
  scheduleRefresh(0);
});
startDateInput.addEventListener("input", () => {
  persistDateTimeControls();
  scheduleRefresh();
});
startDateInput.addEventListener("change", persistDateTimeControls);
startDateInput.addEventListener("change", () => scheduleRefresh(0));
startTimeInput.addEventListener("input", () => {
  persistDateTimeControls();
  scheduleRefresh();
});
startTimeInput.addEventListener("change", persistDateTimeControls);
startTimeInput.addEventListener("change", () => scheduleRefresh(0));
window.addEventListener("resize", refresh);
refresh();
setInterval(refresh, 10000);

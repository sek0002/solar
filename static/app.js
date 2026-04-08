const hoursInput = document.querySelector("#hours");
const windowPreset = document.querySelector("#window-preset");
const startDateInput = document.querySelector("#start-date");
const startTimeInput = document.querySelector("#start-time");
const resetRangeButton = document.querySelector("#reset-range");
const statusCards = document.querySelector("#status-cards");
const latestValues = document.querySelector("#latest-values");
const totalsTableBody = document.querySelector("#totals-table-body");
const refreshText = document.querySelector("#last-refresh");
const batteryText = document.querySelector("#powerpal-battery");
const themeToggle = document.querySelector("#theme-toggle");
const bleChartElement = document.querySelector("#ble-chart");
const cumulativeChartElement = document.querySelector("#cumulative-chart");
const hourlyChartElement = document.querySelector("#hourly-chart");
const weeklyChartElement = document.querySelector("#weekly-chart");
const monthlyChartElement = document.querySelector("#monthly-chart");
const appTimezone = window.SOLAR_MONITOR_CONFIG.timezoneName || "Australia/Melbourne";
const uiStateKey = "solar-monitor-ui-state";

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
  if (windowPreset) {
    const presetValues = Array.from(windowPreset.options).map((option) => option.value);
    windowPreset.value = presetValues.includes(String(clampedHours)) ? String(clampedHours) : "custom";
  }
  updateUiState((state) => {
    state.controls = state.controls || {};
    state.controls.hours = clampedHours;
  });
  return clampedHours;
}

function getDefaultStartDateTime(hours) {
  const clampedHours = clampHours(hours);
  const end = new Date(Date.now() + 3600000);
  end.setSeconds(0, 0);
  return new Date(end.getTime() - clampedHours * 3600000);
}

function applyDefaultStartDateTime(hours) {
  const start = getDefaultStartDateTime(hours);
  startDateInput.value = formatLocalDate(start);
  startTimeInput.value = `${String(start.getHours()).padStart(2, "0")}:${String(start.getMinutes()).padStart(2, "0")}`;
  persistDateTimeControls();
}

function ensureStartInputs() {
  if (startDateInput.value) {
    if (!startTimeInput.value) {
      const fallback = getDefaultStartDateTime(hoursInput.value || window.SOLAR_MONITOR_CONFIG.defaultHours || 24);
      startTimeInput.value = `${String(fallback.getHours()).padStart(2, "0")}:${String(fallback.getMinutes()).padStart(2, "0")}`;
    }
    return;
  }
  applyDefaultStartDateTime(hoursInput.value || window.SOLAR_MONITOR_CONFIG.defaultHours || 24);
}

function persistDateTimeControls() {
  updateUiState((state) => {
    state.controls = state.controls || {};
    state.controls.startDate = startDateInput.value || "";
    state.controls.startTime = startTimeInput.value || "";
  });
}

function buildChartTheme() {
  const dark = getTheme() === "dark";
  return {
    paper_bgcolor: "rgba(0,0,0,0)",
    plot_bgcolor: dark ? "#132134" : "#ffffff",
    margin: { t: 18, r: 18, b: 44, l: 56 },
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
      y: -0.18,
      x: 0,
      font: { color: dark ? "#c5d3e6" : "#5e6b7d", size: 12 },
      bgcolor: "rgba(0,0,0,0)"
    }
  };
}

function formatRatePerMinute(value) {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "n/a";
  }
  return `${Number(value).toFixed(1)} W/min`;
}

function formatKwPerHour(value) {
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

function getWeekKey(dateLike) {
  const parts = getZonedParts(dateLike);
  const date = new Date(
    Number(parts.year),
    Number(parts.month) - 1,
    Number(parts.day),
    0,
    0,
    0,
    0
  );
  const weekday = (date.getDay() + 6) % 7;
  date.setDate(date.getDate() - weekday);
  return `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, "0")}-${String(date.getDate()).padStart(2, "0")}`;
}

function getMonthKey(dateLike) {
  const parts = getZonedParts(dateLike);
  return `${parts.year}-${parts.month}`;
}

function formatDateTime(dateLike) {
  if (!dateLike) {
    return "never";
  }
  return new Date(dateLike).toLocaleString("en-AU", { timeZone: appTimezone });
}

function toChartTime(dateLike) {
  const parts = getZonedParts(dateLike);
  return new Date(
    Number(parts.year),
    Number(parts.month) - 1,
    Number(parts.day),
    Number(parts.hour),
    Number(parts.minute),
    Number(parts.second),
    0
  );
}

function getAxisTitleRate() {
  return "kW/hr";
}

function getAxisTitleSubRate() {
  return "W/min";
}

function buildRateHoverTemplate(label) {
  return `<b>${label}</b><br>%{x}<br>%{y:.3f} kW/hr<br>%{customdata:.1f} W/min<extra></extra>`;
}

function getMinuteOfWeek(dateLike) {
  const parts = getZonedParts(dateLike);
  const date = new Date(
    Number(parts.year),
    Number(parts.month) - 1,
    Number(parts.day),
    Number(parts.hour),
    Number(parts.minute),
    0,
    0
  );
  const day = (date.getDay() + 6) % 7;
  return (day * 24 * 60) + (date.getHours() * 60) + date.getMinutes();
}

function buildFutureTimeline() {
  const now = new Date();
  const points = [];
  for (let offsetMinutes = 10; offsetMinutes <= 60; offsetMinutes += 10) {
    points.push(new Date(now.getTime() + offsetMinutes * 60000));
  }
  return points;
}

function getRecentSlope(series, valueKey) {
  const nowMs = Date.now();
  const pastPoints = series
    .filter((item) => new Date(item.observed_at).getTime() <= nowMs)
    .slice(-4);

  if (pastPoints.length < 2) {
    return null;
  }

  const first = pastPoints[0];
  const last = pastPoints[pastPoints.length - 1];
  const deltaMinutes = (new Date(last.observed_at) - new Date(first.observed_at)) / 60000;
  if (deltaMinutes <= 0) {
    return null;
  }

  const startValue = Number(first[valueKey]);
  const endValue = Number(last[valueKey]);
  return {
    lastObservedAt: new Date(last.observed_at),
    lastValue: endValue,
    slopePerMinute: (endValue - startValue) / deltaMinutes
  };
}

function buildProjectionSeries(series, valueKey) {
  const slope = getRecentSlope(series, valueKey);
  if (!slope) {
    return [];
  }

  return buildFutureTimeline().map((futureTime) => {
    const deltaMinutes = (futureTime - slope.lastObservedAt) / 60000;
    return {
      observed_at: futureTime.toISOString(),
      value: slope.lastValue + (slope.slopePerMinute * deltaMinutes)
    };
  });
}

function buildWeeklyMeanSeries(historySeries, valueKey) {
  if (!historySeries.length) {
    return [];
  }

  const toleranceMinutes = 20;
  return buildFutureTimeline().map((futureTime) => {
    const targetMinute = getMinuteOfWeek(futureTime);
    const matches = historySeries.filter((item) => {
      const historicalMinute = getMinuteOfWeek(item.observed_at);
      const delta = Math.abs(historicalMinute - targetMinute);
      const wrappedDelta = Math.min(delta, (7 * 24 * 60) - delta);
      return wrappedDelta <= toleranceMinutes;
    });

    if (!matches.length) {
      return {
        observed_at: futureTime.toISOString(),
        value: null
      };
    }

    const averageValue = matches.reduce((sum, item) => sum + Number(item[valueKey]), 0) / matches.length;
    return {
      observed_at: futureTime.toISOString(),
      value: averageValue
    };
  });
}

function buildNowLine(xValue) {
  const dark = getTheme() === "dark";
  return {
    type: "line",
    xref: "x",
    yref: "paper",
    x0: xValue,
    x1: xValue,
    y0: 0,
    y1: 1,
    line: {
      color: dark ? "#ff6b6b" : "#d62828",
      width: 2,
      dash: "dash"
    }
  };
}

function getChartHeight() {
  return 360;
}

function resizeCharts() {
  [bleChartElement, cumulativeChartElement, hourlyChartElement, weeklyChartElement, monthlyChartElement].forEach((element) => {
    if (element) {
      Plotly.Plots.resize(element);
    }
  });
}

function formatStatusCard(item) {
  const details = Object.entries(item.details || {})
    .map(([key, value]) => {
      const displayValue = key.endsWith("_at") && value ? formatDateTime(value) : (value === null || value === undefined ? "n/a" : value);
      return `<small><strong>${key}</strong>: ${displayValue}</small>`;
    })
    .join("");

  const errorBlock = item.last_error
    ? `<small><strong>last error</strong>: ${item.last_error}</small>`
    : "";

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

function applyStoredChartState(layout, chartKey, traces) {
  const state = uiState.charts && uiState.charts[chartKey];
  if (!state) {
    return { layout, traces };
  }

  const mergedLayout = { ...layout };
  if (state.xaxisRange && state.xaxisRange.length === 2) {
    mergedLayout.xaxis = {
      ...mergedLayout.xaxis,
      autorange: false,
      range: state.xaxisRange
    };
  }
  if (state.yaxisRange && state.yaxisRange.length === 2) {
    mergedLayout.yaxis = {
      ...mergedLayout.yaxis,
      autorange: false,
      range: state.yaxisRange
    };
  }
  if (state.yaxis2Range && state.yaxis2Range.length === 2) {
    mergedLayout.yaxis2 = {
      ...(mergedLayout.yaxis2 || {}),
      autorange: false,
      range: state.yaxis2Range
    };
  }

  const visibleByName = state.visibleByName || {};
  const mergedTraces = traces.map((trace) => {
    if (!trace.name || !(trace.name in visibleByName)) {
      return trace;
    }
    return { ...trace, visible: visibleByName[trace.name] ? true : "legendonly" };
  });

  return { layout: mergedLayout, traces: mergedTraces };
}

function captureChartState(element, chartKey) {
  if (!element || element.dataset.stateBound === "true") {
    return;
  }

  element.dataset.stateBound = "true";

  element.on("plotly_relayout", () => {
    const layout = element.layout || {};
    updateUiState((state) => {
      state.charts = state.charts || {};
      state.charts[chartKey] = {
        ...(state.charts[chartKey] || {}),
        xaxisRange: layout.xaxis && Array.isArray(layout.xaxis.range) ? [...layout.xaxis.range] : null,
        yaxisRange: layout.yaxis && Array.isArray(layout.yaxis.range) ? [...layout.yaxis.range] : null,
        yaxis2Range: layout.yaxis2 && Array.isArray(layout.yaxis2.range) ? [...layout.yaxis2.range] : null
      };
    });
  });

  element.on("plotly_restyle", () => {
    const visibleByName = Object.fromEntries(
      (element.data || [])
        .filter((trace) => trace.name)
        .map((trace) => [trace.name, trace.visible !== "legendonly"])
    );
    updateUiState((state) => {
      state.charts = state.charts || {};
      state.charts[chartKey] = {
        ...(state.charts[chartKey] || {}),
        visibleByName
      };
    });
  });
}

function formatMetricReading(label, value) {
  return `
    <div class="metric-reading">
      <span class="metric-reading-label">${label}</span>
      <strong class="metric-reading-main">${formatKwPerHour(value)}</strong>
      <small class="metric-reading-sub">${formatRatePerMinute(value)}</small>
    </div>
  `;
}

function formatMetricCard(item) {
  const isImputed = item.raw_payload && item.raw_payload.imputed;
  if (item.source === "byd_ev") {
    const payload = item.raw_payload || {};
    const etaMinutes = payload.time_to_full_minutes;
    const etaText = etaMinutes === null || etaMinutes === undefined
      ? "n/a"
      : `${Math.floor(Number(etaMinutes) / 60)}h ${Number(etaMinutes) % 60}m`;
    return `
      <article class="metric-card">
        <span>byd_ev</span>
        ${formatMetricReading("BYD EV", item.grid_usage_watts)}
        <small>SoC: ${payload.soc_percent === null || payload.soc_percent === undefined ? "n/a" : `${Number(payload.soc_percent).toFixed(0)}%`}</small>
        <small>Range: ${payload.range_km === null || payload.range_km === undefined ? "n/a" : `${Number(payload.range_km).toFixed(0)} km`}</small>
        <small>Charge state: ${payload.charging_state || "n/a"}</small>
        <small>Connected: ${payload.is_connected ? "yes" : "no"}</small>
        <small>ETA: ${etaText}</small>
        <small>Mileage: ${payload.total_mileage_km === null || payload.total_mileage_km === undefined ? "n/a" : `${Number(payload.total_mileage_km).toFixed(0)} km`}</small>
        <small>${formatDateTime(item.observed_at)}</small>
      </article>
    `;
  }
  return `
    <article class="metric-card">
      <span>${item.source}</span>
      ${formatMetricReading("Grid", item.grid_usage_watts)}
      ${formatMetricReading("Solar", item.solar_generation_watts)}
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

  const timeline = items
    .filter((item) =>
      (item.source === "local_site" && item.solar_generation_watts !== null) ||
      (item.source === "ble" && item.grid_usage_watts !== null)
    )
    .sort((left, right) => new Date(left.observed_at) - new Date(right.observed_at));

  const netItems = [];
  let latestSolar = null;
  let latestGrid = null;
  let latestSolarImputed = false;
  let latestGridImputed = false;
  let solarIndex = 0;
  let gridIndex = 0;

  for (const point of timeline) {
    const pointTime = new Date(point.observed_at).getTime();

    while (
      solarIndex < solarItems.length &&
      new Date(solarItems[solarIndex].observed_at).getTime() <= pointTime
    ) {
      latestSolar = Number(solarItems[solarIndex].solar_generation_watts);
      latestSolarImputed = Boolean(solarItems[solarIndex].raw_payload && solarItems[solarIndex].raw_payload.imputed);
      solarIndex += 1;
    }

    while (
      gridIndex < bleGridItems.length &&
      new Date(bleGridItems[gridIndex].observed_at).getTime() <= pointTime
    ) {
      latestGrid = Number(bleGridItems[gridIndex].grid_usage_watts);
      latestGridImputed = Boolean(bleGridItems[gridIndex].raw_payload && bleGridItems[gridIndex].raw_payload.imputed);
      gridIndex += 1;
    }

    if (latestSolar === null || latestGrid === null) {
      continue;
    }

    const lastNet = netItems.length ? netItems[netItems.length - 1] : null;
    const nextCombinedValue = latestSolar + latestGrid;
    if (lastNet && lastNet.observed_at === point.observed_at && lastNet.net_power_watts === nextCombinedValue) {
      continue;
    }

    netItems.push({
      observed_at: point.observed_at,
      solar_generation_watts: latestSolar,
      grid_usage_watts: latestGrid,
      net_power_watts: nextCombinedValue,
      imputed: latestSolarImputed || latestGridImputed
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
    const dayKey = getDayKey(currentDate);

    if (dayKey !== currentDayKey) {
      cumulative = 0;
      currentDayKey = dayKey;
    }

    if (index > 0) {
      const previous = series[index - 1];
      const previousDate = new Date(previous.observed_at);
      const previousDayKey = getDayKey(previousDate);
      const deltaMinutes = (currentDate - previousDate) / 60000;
      const averageRate = (Number(previous[valueKey]) + Number(current[valueKey])) / 2;
      if (deltaMinutes > 0 && previousDayKey === dayKey) {
        cumulative += ratePerMinuteToKwh(averageRate, deltaMinutes);
      }
    }

    points.push({
      observed_at: current.observed_at,
      cumulative_kwh: cumulative
    });
  }

  return points;
}

function buildEnergySegments(series, valueKey) {
  const segments = [];
  for (let index = 1; index < series.length; index += 1) {
    const previous = series[index - 1];
    const current = series[index];
    const previousDate = new Date(previous.observed_at);
    const currentDate = new Date(current.observed_at);
    const deltaMinutes = (currentDate - previousDate) / 60000;
    if (deltaMinutes <= 0) {
      continue;
    }
    const averageRate = (Number(previous[valueKey]) + Number(current[valueKey])) / 2;
    segments.push({
      observed_at: current.observed_at,
      energy_kwh: ratePerMinuteToKwh(averageRate, deltaMinutes)
    });
  }
  return segments;
}

function aggregateEnergyByBucket(segments, keyBuilder) {
  const buckets = new Map();
  segments.forEach((segment) => {
    const key = keyBuilder(segment.observed_at);
    buckets.set(key, (buckets.get(key) || 0) + Number(segment.energy_kwh || 0));
  });
  return Array.from(buckets.entries()).map(([label, value]) => ({ label, value }));
}

function getBleBatteryPercent(pollers) {
  const blePoller = (pollers || []).find((item) => item.name === "ble");
  const batteryPercent = blePoller && blePoller.details ? blePoller.details.battery_percent : null;
  return Number.isFinite(Number(batteryPercent)) ? Number(batteryPercent) : null;
}

function getTodayAndWeekTotals(items) {
  const now = new Date();
  const todayKey = getDayKey(now);
  const weekStart = new Date(now.getTime() - 7 * 24 * 3600000);
  const monthKey = getMonthKey(now);

  const solarSegments = buildEnergySegments(
    items
      .filter((item) => item.source === "local_site" && item.solar_generation_watts !== null)
      .sort((left, right) => new Date(left.observed_at) - new Date(right.observed_at)),
    "solar_generation_watts"
  );
  const gridSegments = buildEnergySegments(
    items
      .filter((item) => item.source === "ble" && item.grid_usage_watts !== null)
      .sort((left, right) => new Date(left.observed_at) - new Date(right.observed_at)),
    "grid_usage_watts"
  );
  const evSegments = buildEnergySegments(
    items
      .filter((item) => item.source === "byd_ev" && item.grid_usage_watts !== null)
      .sort((left, right) => new Date(left.observed_at) - new Date(right.observed_at)),
    "grid_usage_watts"
  );

  const dailySolar = solarSegments
    .filter((segment) => getDayKey(segment.observed_at) === todayKey)
    .reduce((sum, segment) => sum + segment.energy_kwh, 0);
  const dailyGrid = gridSegments
    .filter((segment) => getDayKey(segment.observed_at) === todayKey)
    .reduce((sum, segment) => sum + segment.energy_kwh, 0);
  const weeklySolar = solarSegments
    .filter((segment) => new Date(segment.observed_at) >= weekStart)
    .reduce((sum, segment) => sum + segment.energy_kwh, 0);
  const weeklyGrid = gridSegments
    .filter((segment) => new Date(segment.observed_at) >= weekStart)
    .reduce((sum, segment) => sum + segment.energy_kwh, 0);
  const monthlySolar = solarSegments
    .filter((segment) => getMonthKey(segment.observed_at) === monthKey)
    .reduce((sum, segment) => sum + segment.energy_kwh, 0);
  const monthlyGrid = gridSegments
    .filter((segment) => getMonthKey(segment.observed_at) === monthKey)
    .reduce((sum, segment) => sum + segment.energy_kwh, 0);
  const dailyEv = evSegments
    .filter((segment) => getDayKey(segment.observed_at) === todayKey)
    .reduce((sum, segment) => sum + segment.energy_kwh, 0);
  const weeklyEv = evSegments
    .filter((segment) => new Date(segment.observed_at) >= weekStart)
    .reduce((sum, segment) => sum + segment.energy_kwh, 0);
  const monthlyEv = evSegments
    .filter((segment) => getMonthKey(segment.observed_at) === monthKey)
    .reduce((sum, segment) => sum + segment.energy_kwh, 0);

  return {
    dailySolar,
    dailyGrid,
    dailyNet: dailySolar - dailyGrid,
    weeklySolar,
    weeklyGrid,
    weeklyNet: weeklySolar - weeklyGrid,
    monthlySolar,
    monthlyGrid,
    monthlyNet: monthlySolar - monthlyGrid,
    dailyEv,
    weeklyEv,
    monthlyEv
  };
}

function renderCumulativeStats(items, pollers = []) {
  const totals = getTodayAndWeekTotals(items);
  totalsTableBody.innerHTML = `
    <tr>
      <td>Daily</td>
      <td>${totals.dailySolar.toFixed(2)} kWh</td>
      <td>${totals.dailyGrid.toFixed(2)} kWh</td>
      <td>${totals.dailyEv.toFixed(2)} kWh</td>
      <td>${totals.dailyNet.toFixed(2)} kWh</td>
    </tr>
    <tr>
      <td>Weekly</td>
      <td>${totals.weeklySolar.toFixed(2)} kWh</td>
      <td>${totals.weeklyGrid.toFixed(2)} kWh</td>
      <td>${totals.weeklyEv.toFixed(2)} kWh</td>
      <td>${totals.weeklyNet.toFixed(2)} kWh</td>
    </tr>
    <tr>
      <td>Monthly</td>
      <td>${totals.monthlySolar.toFixed(2)} kWh</td>
      <td>${totals.monthlyGrid.toFixed(2)} kWh</td>
      <td>${totals.monthlyEv.toFixed(2)} kWh</td>
      <td>${totals.monthlyNet.toFixed(2)} kWh</td>
    </tr>
  `;
}

function buildRateChart(element, chartKey, title, items, valueKey, colors, historySeries = []) {
  const chartTheme = buildChartTheme();
  const dark = getTheme() === "dark";
  const xValues = items.map((item) => toChartTime(item.observed_at));
  const yKw = items.map((item) => ratePerMinuteToKwPerHour(item[valueKey]));
  const yRate = items.map((item) => Number(item[valueKey]));
  const projectedSeries = buildProjectionSeries(items, valueKey);
  const weeklyMeanSeries = buildWeeklyMeanSeries(historySeries, valueKey);
  const nowX = xValues.length ? toChartTime(new Date()) : null;
  const traces = [
    {
      x: xValues,
      y: yKw,
      customdata: yRate,
      mode: "lines",
      name: title,
      line: { color: colors.line, width: 1.6, shape: "linear" },
      fill: "tozeroy",
      fillcolor: colors.fill,
      hovertemplate: buildRateHoverTemplate(title)
    },
    {
      x: xValues,
      y: yRate,
      yaxis: "y2",
      mode: "lines",
      name: `${title} raw`,
      showlegend: false,
      hoverinfo: "skip",
      line: { color: "rgba(0,0,0,0)", width: 0 }
    },
    {
      x: projectedSeries.map((item) => toChartTime(item.observed_at)),
      y: projectedSeries.map((item) => ratePerMinuteToKwPerHour(item.value)),
      customdata: projectedSeries.map((item) => Number(item.value)),
      mode: "lines",
      name: `${title} trend`,
      line: { color: colors.line, width: 1.5, dash: "dot" },
      hovertemplate: buildRateHoverTemplate(`${title} trend`)
    },
    {
      x: weeklyMeanSeries.filter((item) => item.value !== null).map((item) => toChartTime(item.observed_at)),
      y: weeklyMeanSeries.filter((item) => item.value !== null).map((item) => ratePerMinuteToKwPerHour(item.value)),
      customdata: weeklyMeanSeries.filter((item) => item.value !== null).map((item) => Number(item.value)),
      mode: "lines",
      name: `${title} weekly mean`,
      line: { color: colors.line, width: 1.2, dash: "dash" },
      opacity: 0.35,
      hovertemplate: buildRateHoverTemplate(`${title} weekly mean`)
    }
  ];

  const layout = {
    ...chartTheme,
    height: getChartHeight(),
    shapes: nowX ? [buildNowLine(nowX)] : [],
    uirevision: chartKey,
    title: {
      text: title,
      font: { color: dark ? "#edf4ff" : "#263445", size: 16 }
    },
    yaxis: {
      ...chartTheme.yaxis,
      title: getAxisTitleRate(),
      zeroline: true,
      zerolinecolor: dark ? "rgba(124, 147, 180, 0.42)" : "rgba(164, 179, 201, 0.42)",
      rangemode: "tozero"
    },
    yaxis2: {
      title: getAxisTitleSubRate(),
      overlaying: "y",
      side: "right",
      tickfont: { color: dark ? "#97abc5" : "#7a8797", size: 11 },
      titlefont: { color: dark ? "#97abc5" : "#7a8797" },
      gridcolor: "rgba(0,0,0,0)",
      zeroline: false
    }
  };

  const chartState = applyStoredChartState(layout, chartKey, traces);
  Plotly.react(element, chartState.traces, chartState.layout, {
    responsive: true,
    displaylogo: false,
    modeBarButtonsToRemove: ["lasso2d", "select2d"]
  });
  captureChartState(element, chartKey);
}

function renderBleSolarChart(items, historyItems) {
  const dark = getTheme() === "dark";
  const bleGrid = items
    .filter((item) => item.source === "ble" && item.grid_usage_watts !== null)
    .sort((left, right) => new Date(left.observed_at) - new Date(right.observed_at));
  const bleGridHistory = historyItems
    .filter((item) => item.source === "ble" && item.grid_usage_watts !== null)
    .sort((left, right) => new Date(left.observed_at) - new Date(right.observed_at));
  const siteSolar = items
    .filter((item) => item.source === "local_site" && item.solar_generation_watts !== null)
    .sort((left, right) => new Date(left.observed_at) - new Date(right.observed_at));
  const siteSolarHistory = historyItems
    .filter((item) => item.source === "local_site" && item.solar_generation_watts !== null)
    .sort((left, right) => new Date(left.observed_at) - new Date(right.observed_at));
  const evItems = items
    .filter((item) => item.source === "byd_ev" && item.grid_usage_watts !== null)
    .sort((left, right) => new Date(left.observed_at) - new Date(right.observed_at));
  const evHistoryItems = historyItems
    .filter((item) => item.source === "byd_ev" && item.grid_usage_watts !== null)
    .sort((left, right) => new Date(left.observed_at) - new Date(right.observed_at));

  const chartTheme = buildChartTheme();
  const nowX = bleGrid.length || siteSolar.length || evItems.length ? toChartTime(new Date()) : null;
  const traces = [
    {
      x: bleGrid.map((item) => toChartTime(item.observed_at)),
      y: bleGrid.map((item) => ratePerMinuteToKwPerHour(item.grid_usage_watts)),
      customdata: bleGrid.map((item) => Number(item.grid_usage_watts)),
      mode: "lines",
      name: "BLE grid",
      line: { color: dark ? "#7fb0ff" : "#6f96d8", width: 1.6, shape: "linear" },
      fill: "tozeroy",
      fillcolor: dark ? "rgba(127, 176, 255, 0.16)" : "rgba(111, 150, 216, 0.17)",
      hovertemplate: buildRateHoverTemplate("BLE grid")
    },
    {
      x: bleGrid.map((item) => toChartTime(item.observed_at)),
      y: bleGrid.map((item) => Number(item.grid_usage_watts)),
      yaxis: "y2",
      mode: "lines",
      name: "BLE grid raw",
      showlegend: false,
      hoverinfo: "skip",
      line: { color: "rgba(0,0,0,0)", width: 0 }
    },
    {
      x: buildProjectionSeries(bleGrid, "grid_usage_watts").map((item) => toChartTime(item.observed_at)),
      y: buildProjectionSeries(bleGrid, "grid_usage_watts").map((item) => ratePerMinuteToKwPerHour(item.value)),
      customdata: buildProjectionSeries(bleGrid, "grid_usage_watts").map((item) => Number(item.value)),
      mode: "lines",
      name: "BLE grid trend",
      line: { color: dark ? "#7fb0ff" : "#6f96d8", width: 1.5, dash: "dot" },
      hovertemplate: buildRateHoverTemplate("BLE grid trend")
    },
    {
      x: buildWeeklyMeanSeries(bleGridHistory, "grid_usage_watts").filter((item) => item.value !== null).map((item) => toChartTime(item.observed_at)),
      y: buildWeeklyMeanSeries(bleGridHistory, "grid_usage_watts").filter((item) => item.value !== null).map((item) => ratePerMinuteToKwPerHour(item.value)),
      customdata: buildWeeklyMeanSeries(bleGridHistory, "grid_usage_watts").filter((item) => item.value !== null).map((item) => Number(item.value)),
      mode: "lines",
      name: "BLE grid weekly mean",
      line: { color: dark ? "#7fb0ff" : "#6f96d8", width: 1.2, dash: "dash" },
      opacity: 0.35,
      hovertemplate: buildRateHoverTemplate("BLE grid weekly mean")
    },
    {
      x: siteSolar.map((item) => toChartTime(item.observed_at)),
      y: siteSolar.map((item) => ratePerMinuteToKwPerHour(item.solar_generation_watts)),
      customdata: siteSolar.map((item) => Number(item.solar_generation_watts)),
      mode: "lines",
      name: "Site solar",
      line: { color: dark ? "#8ee29d" : "#7cc98a", width: 1.6, shape: "linear" },
      fill: "tozeroy",
      fillcolor: dark ? "rgba(142, 226, 157, 0.12)" : "rgba(124, 201, 138, 0.12)",
      hovertemplate: buildRateHoverTemplate("Site solar")
    },
    {
      x: siteSolar.map((item) => toChartTime(item.observed_at)),
      y: siteSolar.map((item) => Number(item.solar_generation_watts)),
      yaxis: "y2",
      mode: "lines",
      name: "Site solar raw",
      showlegend: false,
      hoverinfo: "skip",
      line: { color: "rgba(0,0,0,0)", width: 0 }
    },
    {
      x: buildProjectionSeries(siteSolar, "solar_generation_watts").map((item) => toChartTime(item.observed_at)),
      y: buildProjectionSeries(siteSolar, "solar_generation_watts").map((item) => ratePerMinuteToKwPerHour(item.value)),
      customdata: buildProjectionSeries(siteSolar, "solar_generation_watts").map((item) => Number(item.value)),
      mode: "lines",
      name: "Site solar trend",
      line: { color: dark ? "#8ee29d" : "#7cc98a", width: 1.5, dash: "dot" },
      hovertemplate: buildRateHoverTemplate("Site solar trend")
    },
    {
      x: buildWeeklyMeanSeries(siteSolarHistory, "solar_generation_watts").filter((item) => item.value !== null).map((item) => toChartTime(item.observed_at)),
      y: buildWeeklyMeanSeries(siteSolarHistory, "solar_generation_watts").filter((item) => item.value !== null).map((item) => ratePerMinuteToKwPerHour(item.value)),
      customdata: buildWeeklyMeanSeries(siteSolarHistory, "solar_generation_watts").filter((item) => item.value !== null).map((item) => Number(item.value)),
      mode: "lines",
      name: "Site solar weekly mean",
      line: { color: dark ? "#8ee29d" : "#7cc98a", width: 1.2, dash: "dash" },
      opacity: 0.35,
      hovertemplate: buildRateHoverTemplate("Site solar weekly mean")
    },
    {
      x: evItems.map((item) => toChartTime(item.observed_at)),
      y: evItems.map((item) => ratePerMinuteToKwPerHour(item.grid_usage_watts)),
      customdata: evItems.map((item) => Number(item.grid_usage_watts)),
      mode: "lines",
      name: "BYD EV",
      line: { color: dark ? "#ffb45b" : "#d6882e", width: 1.6, shape: "linear" },
      hovertemplate: buildRateHoverTemplate("BYD EV")
    },
    {
      x: evItems.map((item) => toChartTime(item.observed_at)),
      y: evItems.map((item) => Number(item.grid_usage_watts)),
      yaxis: "y2",
      mode: "lines",
      name: "BYD EV raw",
      showlegend: false,
      hoverinfo: "skip",
      line: { color: "rgba(0,0,0,0)", width: 0 }
    },
    {
      x: buildProjectionSeries(evItems, "grid_usage_watts").map((item) => toChartTime(item.observed_at)),
      y: buildProjectionSeries(evItems, "grid_usage_watts").map((item) => ratePerMinuteToKwPerHour(item.value)),
      customdata: buildProjectionSeries(evItems, "grid_usage_watts").map((item) => Number(item.value)),
      mode: "lines",
      name: "BYD EV trend",
      line: { color: dark ? "#ffb45b" : "#d6882e", width: 1.5, dash: "dot" },
      hovertemplate: buildRateHoverTemplate("BYD EV trend")
    },
    {
      x: buildWeeklyMeanSeries(evHistoryItems, "grid_usage_watts").filter((item) => item.value !== null).map((item) => toChartTime(item.observed_at)),
      y: buildWeeklyMeanSeries(evHistoryItems, "grid_usage_watts").filter((item) => item.value !== null).map((item) => ratePerMinuteToKwPerHour(item.value)),
      customdata: buildWeeklyMeanSeries(evHistoryItems, "grid_usage_watts").filter((item) => item.value !== null).map((item) => Number(item.value)),
      mode: "lines",
      name: "BYD EV weekly mean",
      line: { color: dark ? "#ffb45b" : "#d6882e", width: 1.2, dash: "dash" },
      opacity: 0.35,
      hovertemplate: buildRateHoverTemplate("BYD EV weekly mean")
    }
  ];

  const layout = {
    ...chartTheme,
    height: getChartHeight(),
    shapes: nowX ? [buildNowLine(nowX)] : [],
    uirevision: "ble-solar-rate",
    title: {
      text: "BLE grid, site solar, and BYD EV",
      font: { color: dark ? "#edf4ff" : "#263445", size: 16 }
    },
    yaxis: {
      ...chartTheme.yaxis,
      title: getAxisTitleRate(),
      zeroline: true,
      zerolinecolor: dark ? "rgba(124, 147, 180, 0.42)" : "rgba(164, 179, 201, 0.42)",
      rangemode: "tozero"
    },
    yaxis2: {
      title: getAxisTitleSubRate(),
      overlaying: "y",
      side: "right",
      tickfont: { color: dark ? "#97abc5" : "#7a8797", size: 11 },
      titlefont: { color: dark ? "#97abc5" : "#7a8797" },
      gridcolor: "rgba(0,0,0,0)",
      zeroline: false
    }
  };

  const chartState = applyStoredChartState(layout, "ble-solar-rate", traces);
  Plotly.react(bleChartElement, chartState.traces, chartState.layout, {
    responsive: true,
    displaylogo: false,
    modeBarButtonsToRemove: ["lasso2d", "select2d"]
  });
  captureChartState(bleChartElement, "ble-solar-rate");
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
  const evItems = items
    .filter((item) => item.source === "byd_ev" && item.grid_usage_watts !== null)
    .sort((left, right) => new Date(left.observed_at) - new Date(right.observed_at));

  const solarKwh = integrateSeriesKwh(solarItems, "solar_generation_watts");
  const gridKwh = integrateSeriesKwh(bleGridItems, "grid_usage_watts");
  const evKwh = integrateSeriesKwh(evItems, "grid_usage_watts");
  const xValues = [
    ...solarKwh.map((item) => toChartTime(item.observed_at)),
    ...gridKwh.map((item) => toChartTime(item.observed_at)),
    ...evKwh.map((item) => toChartTime(item.observed_at))
  ];

  const traces = [
    {
      x: solarKwh.map((item) => toChartTime(item.observed_at)),
      y: solarKwh.map((item) => item.cumulative_kwh),
      mode: "lines",
      name: "Site solar cumulative",
      line: { color: dark ? "#8ee29d" : "#7cc98a", width: 1.6 },
      fill: "tozeroy",
      fillcolor: dark ? "rgba(142, 226, 157, 0.14)" : "rgba(124, 201, 138, 0.12)"
    },
    {
      x: gridKwh.map((item) => toChartTime(item.observed_at)),
      y: gridKwh.map((item) => item.cumulative_kwh),
      mode: "lines",
      name: "BLE grid cumulative",
      line: { color: dark ? "#7fb0ff" : "#6f96d8", width: 1.5 },
      fill: "tozeroy",
      fillcolor: dark ? "rgba(127, 176, 255, 0.12)" : "rgba(111, 150, 216, 0.09)"
    },
    {
      x: evKwh.map((item) => toChartTime(item.observed_at)),
      y: evKwh.map((item) => item.cumulative_kwh),
      mode: "lines",
      name: "BYD EV cumulative",
      line: { color: dark ? "#ffb45b" : "#d6882e", width: 1.5 },
      fill: "tozeroy",
      fillcolor: dark ? "rgba(255, 180, 91, 0.12)" : "rgba(214, 136, 46, 0.10)"
    }
  ];

  const layout = {
    ...chartTheme,
    height: getChartHeight(),
    shapes: xValues.length ? [buildNowLine(toChartTime(new Date()))] : [],
    uirevision: "cumulative-chart",
    title: {
      text: "Cumulative energy",
      font: { color: dark ? "#edf4ff" : "#263445", size: 16 }
    },
    yaxis: {
      ...chartTheme.yaxis,
      title: "kWh",
      rangemode: "tozero"
    }
  };

  const chartState = applyStoredChartState(layout, "cumulative", traces);
  Plotly.react(cumulativeChartElement, chartState.traces, chartState.layout, {
    responsive: true,
    displaylogo: false,
    modeBarButtonsToRemove: ["lasso2d", "select2d"]
  });
  captureChartState(cumulativeChartElement, "cumulative");
}

function renderEnergyBars(element, chartKey, title, bars) {
  const chartTheme = buildChartTheme();
  const dark = getTheme() === "dark";
  const traces = [
    {
      x: bars.map((item) => item.label),
      y: bars.map((item) => item.grid),
      type: "bar",
      name: "BLE grid",
      marker: { color: dark ? "#7fb0ff" : "#6f96d8" },
      hovertemplate: "<b>BLE grid</b><br>%{x}<br>%{y:.3f} kWh<extra></extra>"
    },
    {
      x: bars.map((item) => item.label),
      y: bars.map((item) => item.solar),
      type: "bar",
      name: "Site solar",
      marker: { color: dark ? "#8ee29d" : "#7cc98a" },
      hovertemplate: "<b>Site solar</b><br>%{x}<br>%{y:.3f} kWh<extra></extra>"
    },
    {
      x: bars.map((item) => item.label),
      y: bars.map((item) => item.ev),
      type: "bar",
      name: "BYD EV",
      marker: { color: dark ? "#ffb45b" : "#d6882e" },
      hovertemplate: "<b>BYD EV</b><br>%{x}<br>%{y:.3f} kWh<extra></extra>"
    }
  ];

  const layout = {
    ...chartTheme,
    height: getChartHeight(),
    barmode: "group",
    uirevision: chartKey,
    title: {
      text: title,
      font: { color: dark ? "#edf4ff" : "#263445", size: 16 }
    },
    yaxis: {
      ...chartTheme.yaxis,
      title: "kWh",
      rangemode: "tozero"
    }
  };

  const chartState = applyStoredChartState(layout, chartKey, traces);
  Plotly.react(element, chartState.traces, chartState.layout, {
    responsive: true,
    displaylogo: false,
    modeBarButtonsToRemove: ["lasso2d", "select2d"]
  });
  captureChartState(element, chartKey);
}

function renderEnergyBreakdowns(items) {
  const solarItems = items
    .filter((item) => item.source === "local_site" && item.solar_generation_watts !== null)
    .sort((left, right) => new Date(left.observed_at) - new Date(right.observed_at));
  const bleGridItems = items
    .filter((item) => item.source === "ble" && item.grid_usage_watts !== null)
    .sort((left, right) => new Date(left.observed_at) - new Date(right.observed_at));
  const bydItems = items
    .filter((item) => item.source === "byd_ev" && item.grid_usage_watts !== null)
    .sort((left, right) => new Date(left.observed_at) - new Date(right.observed_at));

  const solarHourBars = aggregateEnergyByBucket(buildEnergySegments(solarItems, "solar_generation_watts"), getHourKey);
  const gridHourBars = aggregateEnergyByBucket(buildEnergySegments(bleGridItems, "grid_usage_watts"), getHourKey);
  const bydHourBars = aggregateEnergyByBucket(buildEnergySegments(bydItems, "grid_usage_watts"), getHourKey);
  const solarWeekBars = aggregateEnergyByBucket(buildEnergySegments(solarItems, "solar_generation_watts"), getWeekKey);
  const gridWeekBars = aggregateEnergyByBucket(buildEnergySegments(bleGridItems, "grid_usage_watts"), getWeekKey);
  const bydWeekBars = aggregateEnergyByBucket(buildEnergySegments(bydItems, "grid_usage_watts"), getWeekKey);
  const solarMonthBars = aggregateEnergyByBucket(buildEnergySegments(solarItems, "solar_generation_watts"), getMonthKey);
  const gridMonthBars = aggregateEnergyByBucket(buildEnergySegments(bleGridItems, "grid_usage_watts"), getMonthKey);
  const bydMonthBars = aggregateEnergyByBucket(buildEnergySegments(bydItems, "grid_usage_watts"), getMonthKey);

  function combineBars(solarBars, gridBars, evBars) {
    const map = new Map();
    solarBars.forEach((item) => {
      map.set(item.label, { label: item.label, solar: item.value, grid: 0, ev: 0 });
    });
    gridBars.forEach((item) => {
      const existing = map.get(item.label) || { label: item.label, solar: 0, grid: 0, ev: 0 };
      existing.grid = item.value;
      map.set(item.label, existing);
    });
    evBars.forEach((item) => {
      const existing = map.get(item.label) || { label: item.label, solar: 0, grid: 0, ev: 0 };
      existing.ev = item.value;
      map.set(item.label, existing);
    });
    return Array.from(map.values()).sort((left, right) => left.label.localeCompare(right.label));
  }

  renderEnergyBars(hourlyChartElement, "hourly-bars", "Hourly cumulative split", combineBars(solarHourBars, gridHourBars, bydHourBars));
  renderEnergyBars(weeklyChartElement, "weekly-bars", "Weekly cumulative split", combineBars(solarWeekBars, gridWeekBars, bydWeekBars));
  renderEnergyBars(monthlyChartElement, "monthly-bars", "Monthly cumulative split", combineBars(solarMonthBars, gridMonthBars, bydMonthBars));
}

function renderEmptyCharts() {
  const chartTheme = buildChartTheme();
  const dark = getTheme() === "dark";
  const emptyAnnotation = {
    text: "No data in the selected window",
    showarrow: false,
    font: { color: dark ? "#97abc5" : "#7a8797", size: 14 },
    xref: "paper",
    yref: "paper",
    x: 0.5,
    y: 0.5
  };

  function emptyRateChart(element, chartKey, title) {
    const state = applyStoredChartState({
      ...chartTheme,
      height: getChartHeight(),
      annotations: [emptyAnnotation],
      uirevision: chartKey,
      title: {
        text: title,
        font: { color: dark ? "#edf4ff" : "#263445", size: 16 }
      },
      yaxis: { ...chartTheme.yaxis, title: getAxisTitleRate() },
      yaxis2: {
        title: getAxisTitleSubRate(),
        overlaying: "y",
        side: "right",
        tickfont: { color: dark ? "#97abc5" : "#7a8797", size: 11 },
        titlefont: { color: dark ? "#97abc5" : "#7a8797" },
        gridcolor: "rgba(0,0,0,0)",
        zeroline: false
      }
    }, chartKey, []);
    Plotly.react(element, state.traces, state.layout, { responsive: true, displaylogo: false, modeBarButtonsToRemove: ["lasso2d", "select2d"] });
    captureChartState(element, chartKey);
  }

  function emptyEnergyChart(element, chartKey, title) {
    const state = applyStoredChartState({
      ...chartTheme,
      height: getChartHeight(),
      annotations: [emptyAnnotation],
      uirevision: chartKey,
      title: {
        text: title,
        font: { color: dark ? "#edf4ff" : "#263445", size: 16 }
      },
      yaxis: { ...chartTheme.yaxis, title: "kWh", rangemode: "tozero" }
    }, chartKey, []);
    Plotly.react(element, state.traces, state.layout, { responsive: true, displaylogo: false, modeBarButtonsToRemove: ["lasso2d", "select2d"] });
    captureChartState(element, chartKey);
  }

  emptyRateChart(bleChartElement, "ble-solar-rate", "BLE grid, site solar, and BYD EV");
  emptyEnergyChart(cumulativeChartElement, "cumulative", "Cumulative energy");
  emptyEnergyChart(hourlyChartElement, "hourly-bars", "Hourly cumulative split");
  emptyEnergyChart(weeklyChartElement, "weekly-bars", "Weekly cumulative split");
  emptyEnergyChart(monthlyChartElement, "monthly-bars", "Monthly cumulative split");
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
    const [statusResponse, samplesResponse, historyResponse] = await Promise.all([
      fetch("/api/status"),
      fetch(`/api/samples?hours=${hours}&start=${encodeURIComponent(safeStart.toISOString())}&end=${encodeURIComponent(end.toISOString())}`),
      fetch(`/api/samples?hours=168&end=${encodeURIComponent(new Date().toISOString())}`)
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
    latestValues.innerHTML = statusPayload.latest_samples
      .filter((item) => item.source !== "tuya_ev")
      .map(formatMetricCard)
      .join("");
    const batteryPercent = getBleBatteryPercent(statusPayload.pollers);
    batteryText.textContent = batteryPercent === null ? "Battery n/a" : `Battery ${batteryPercent}%`;

    if (!items.length) {
      totalsTableBody.innerHTML = `
        <tr><td>Daily</td><td>0.00 kWh</td><td>0.00 kWh</td><td>0.00 kWh</td><td>0.00 kWh</td></tr>
        <tr><td>Weekly</td><td>0.00 kWh</td><td>0.00 kWh</td><td>0.00 kWh</td><td>0.00 kWh</td></tr>
        <tr><td>Monthly</td><td>0.00 kWh</td><td>0.00 kWh</td><td>0.00 kWh</td><td>0.00 kWh</td></tr>
      `;
      renderEmptyCharts();
      refreshText.textContent = "No data in selected window";
      return;
    }

    renderCumulativeStats(items, statusPayload.pollers);
    renderBleSolarChart(items, historyItems);
    renderCumulativeChart(items);
    renderEnergyBreakdowns(items);
    refreshText.textContent = `Updated ${new Date().toLocaleTimeString("en-AU", { timeZone: appTimezone })}`;
  } catch (error) {
    console.error("Refresh failed", error);
    renderEmptyCharts();
    refreshText.textContent = "Refresh failed";
    batteryText.textContent = "Battery n/a";
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
syncWindowControls((uiState.controls && uiState.controls.hours) || window.SOLAR_MONITOR_CONFIG.defaultHours || 24);
persistDateTimeControls();
bindStatusCardPersistence();

themeToggle.addEventListener("click", () => {
  const nextTheme = getTheme() === "dark" ? "light" : "dark";
  setTheme(nextTheme);
  scheduleRefresh(0);
});

windowPreset.addEventListener("change", () => {
  if (windowPreset.value !== "custom") {
    syncWindowControls(windowPreset.value);
    scheduleRefresh(0);
  }
});

hoursInput.addEventListener("input", () => {
  syncWindowControls(hoursInput.value);
  scheduleRefresh(200);
});
hoursInput.addEventListener("change", () => {
  syncWindowControls(hoursInput.value);
  scheduleRefresh(0);
});

[startDateInput, startTimeInput].forEach((input) => {
  input.addEventListener("input", () => {
    persistDateTimeControls();
    scheduleRefresh(200);
  });
  input.addEventListener("change", () => {
    persistDateTimeControls();
    scheduleRefresh(0);
  });
});

resetRangeButton.addEventListener("click", () => {
  applyDefaultStartDateTime(hoursInput.value || window.SOLAR_MONITOR_CONFIG.defaultHours || 24);
  scheduleRefresh(0);
});

window.addEventListener("resize", resizeCharts);
refresh();
setInterval(refresh, 10000);

const hoursInput = document.querySelector("#hours");
const windowPreset = document.querySelector("#window-preset");
const startDateInput = document.querySelector("#start-date");
const startTimeInput = document.querySelector("#start-time");
const resetRangeButton = document.querySelector("#reset-range");
const statusCards = document.querySelector("#status-cards");
const collectorStrip = document.querySelector("#collector-strip");
const latestValues = document.querySelector("#latest-values");
const totalsTableBody = document.querySelector("#totals-table-body");
const refreshText = document.querySelector("#last-refresh");
const batteryText = document.querySelector("#powerpal-battery");
const bleBatteryFill = document.querySelector("#ble-battery-fill");
const evBatteryFill = document.querySelector("#ev-battery-fill");
const evBatteryLabel = document.querySelector("#ev-battery-label");
const topbarGauge = document.querySelector("#topbar-gauge");
const topbarSolarValue = document.querySelector("#topbar-solar-value");
const topbarBleValue = document.querySelector("#topbar-ble-value");
const topbarBydGauge = document.querySelector("#topbar-byd-gauge");
const topbarBydValue = document.querySelector("#topbar-byd-value");
const topbarBydSubvalue = document.querySelector("#topbar-byd-subvalue");
const themeToggle = document.querySelector("#theme-toggle");
const bleChartElement = document.querySelector("#ble-chart");
const cumulativeChartElement = document.querySelector("#cumulative-chart");
const hourlyChartElement = document.querySelector("#hourly-chart");
const dailyChartElement = document.querySelector("#daily-chart");
const weeklyChartElement = document.querySelector("#weekly-chart");
const appTimezone = window.SOLAR_MONITOR_CONFIG.timezoneName || "Australia/Melbourne";
const uiStateKey = "solar-monitor-ui-state";
const appCacheVersionKey = "solar-monitor-cache-version";
const appCacheVersion = (window.SOLAR_PWA && window.SOLAR_PWA.appVersion) || "dev";
const pageLoadDefaultHours = 12;

function resetVersionedClientCache() {
  localStorage.removeItem(uiStateKey);
  if ("caches" in window) {
    caches.keys()
      .then((keys) => Promise.all(keys.map((key) => caches.delete(key))))
      .catch((error) => console.warn("Unable to clear cached assets", error));
  }
}

const storedCacheVersion = localStorage.getItem(appCacheVersionKey);
if (storedCacheVersion !== appCacheVersion) {
  resetVersionedClientCache();
  localStorage.setItem(appCacheVersionKey, appCacheVersion);
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

function getTimeZoneOffsetMs(dateLike, timeZone = appTimezone) {
  const date = new Date(dateLike);
  const formatter = new Intl.DateTimeFormat("en-US", {
    timeZone,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hourCycle: "h23"
  });
  const parts = Object.fromEntries(
    formatter.formatToParts(date)
      .filter((part) => part.type !== "literal")
      .map((part) => [part.type, part.value])
  );
  const zonedUtcMs = Date.UTC(
    Number(parts.year),
    Number(parts.month) - 1,
    Number(parts.day),
    Number(parts.hour),
    Number(parts.minute),
    Number(parts.second),
    0
  );
  return zonedUtcMs - date.getTime();
}

function buildAppDateTime(dateValue, timeValue = "00:00") {
  if (!dateValue) {
    return new Date();
  }
  const [year, month, day] = dateValue.split("-").map(Number);
  const [hours, minutes] = (timeValue || "00:00").split(":").map(Number);
  const utcGuess = new Date(Date.UTC(year, (month || 1) - 1, day || 1, hours || 0, minutes || 0, 0, 0));
  const firstPass = new Date(utcGuess.getTime() - getTimeZoneOffsetMs(utcGuess));
  return new Date(firstPass.getTime() - getTimeZoneOffsetMs(firstPass) + getTimeZoneOffsetMs(utcGuess));
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

function getRangeMode() {
  return uiState.controls && uiState.controls.rangeMode === "fixed" ? "fixed" : "live";
}

function isFixedRange() {
  return getRangeMode() === "fixed";
}

function setRangeMode(mode) {
  updateUiState((state) => {
    state.controls = state.controls || {};
    state.controls.rangeMode = mode === "fixed" ? "fixed" : "live";
  });
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
  const end = new Date();
  end.setSeconds(0, 0);
  return new Date(end.getTime() - clampedHours * 3600000);
}

function formatLocalTime(date) {
  const parts = getZonedParts(date);
  return `${parts.hour}:${parts.minute}`;
}

function syncDisplayedStart(date) {
  startDateInput.value = formatLocalDate(date);
  startTimeInput.value = formatLocalTime(date);
}

function applyDefaultStartDateTime(hours) {
  const start = getDefaultStartDateTime(hours);
  syncDisplayedStart(start);
  persistDateTimeControls();
}

function ensureStartInputs() {
  if (startDateInput.value) {
    if (!startTimeInput.value) {
      const fallback = getDefaultStartDateTime(hoursInput.value || window.SOLAR_MONITOR_CONFIG.defaultHours || 24);
      startTimeInput.value = formatLocalTime(fallback);
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
    margin: { t: 12, r: 12, b: 34, l: 46 },
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

function formatRatePerMinute(value) {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "n/a";
  }
  return `${Number(value).toFixed(1)} W`;
}

function formatKwPerHour(value) {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "n/a";
  }
  return `${ratePerMinuteToKwPerHour(value).toFixed(3)} kW`;
}

function formatWatts(value) {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "n/a";
  }
  return `${Number(value).toFixed(1)} W`;
}

function formatGaugeKwPerHour(value) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) {
    return "n/a";
  }
  return ratePerMinuteToKwPerHour(value).toFixed(2);
}

function ratePerMinuteToKwh(ratePerMinute, deltaMinutes) {
  return (Number(ratePerMinute) * deltaMinutes) / 60000;
}

function ratePerMinuteToKwPerHour(value) {
  return Number(value) / 1000;
}

function wattsToKw(value) {
  return Number(value) / 1000;
}

function clamp(value, min, max) {
  return Math.min(max, Math.max(min, value));
}

function getInfernoSolarColor(ratePerMinute, maxKwPerHour = 5) {
  if (ratePerMinute === null || ratePerMinute === undefined || Number.isNaN(Number(ratePerMinute))) {
    return "#4c1d4b";
  }
  const stops = [
    { t: 0.0, color: [27, 12, 65] },
    { t: 0.25, color: [100, 19, 113] },
    { t: 0.5, color: [187, 55, 84] },
    { t: 0.75, color: [249, 142, 8] },
    { t: 1.0, color: [252, 255, 164] }
  ];
  const normalized = clamp(ratePerMinuteToKwPerHour(ratePerMinute) / maxKwPerHour, 0, 1);
  const upperIndex = stops.findIndex((stop) => stop.t >= normalized);
  const upper = upperIndex === -1 ? stops[stops.length - 1] : stops[upperIndex];
  const lower = upperIndex <= 0 ? stops[0] : stops[upperIndex - 1];
  const span = upper.t - lower.t || 1;
  const mix = (normalized - lower.t) / span;
  const channels = lower.color.map((channel, index) => Math.round(channel + (upper.color[index] - channel) * mix));
  return `rgb(${channels[0]}, ${channels[1]}, ${channels[2]})`;
}

function getLatestRateBySource(samples, source, valueKey) {
  const latestItem = (samples || [])
    .filter((item) => item.source === source && item[valueKey] !== null && item[valueKey] !== undefined)
    .sort((left, right) => new Date(right.observed_at) - new Date(left.observed_at))[0];
  return latestItem ? Number(latestItem[valueKey]) : null;
}

function renderTopbarGauge(samples) {
  const solarRate = getLatestRateBySource(samples, "local_site", "solar_generation_watts");
  const bleRate = getLatestRateBySource(samples, "ble", "grid_usage_watts");
  const bleProgress = clamp((bleRate === null ? 0 : ratePerMinuteToKwPerHour(bleRate) / 10), 0, 1);

  if (topbarGauge) {
    topbarGauge.style.setProperty("--ring-progress", `${bleProgress}turn`);
    topbarGauge.style.setProperty("--core-fill", getInfernoSolarColor(solarRate, 5));
    topbarGauge.style.setProperty("--inner-ring-stroke", "rgba(240, 244, 255, 0.1)");
  }
  if (topbarSolarValue) {
    topbarSolarValue.textContent = formatGaugeKwPerHour(solarRate);
  }
  if (topbarBleValue) {
    topbarBleValue.textContent = bleRate === null ? "BLE n/a" : `BLE ${formatGaugeKwPerHour(bleRate)}`;
  }
}

function renderBydTopbarGauge(samples, pollers) {
  const bydSample = (samples || []).find((item) => item.source === "byd_ev");
  const glWatts = bydSample ? getBydPowerWatts(bydSample) : null;
  const glRate = glWatts === null ? null : glWatts / 60;
  const socPercent = getBydSocPercent(samples, pollers);
  const socProgress = clamp((socPercent === null ? 0 : socPercent / 100), 0, 1);

  if (topbarBydGauge) {
    topbarBydGauge.style.setProperty("--ring-progress", `${socProgress}turn`);
    topbarBydGauge.style.setProperty("--core-fill", getInfernoSolarColor(glRate, 3));
    topbarBydGauge.style.setProperty("--inner-ring-stroke", "rgba(255, 214, 181, 0.09)");
  }
  if (topbarBydValue) {
    topbarBydValue.textContent = formatGaugeKwPerHour(glRate);
  }
  if (topbarBydSubvalue) {
    topbarBydSubvalue.textContent = socPercent === null ? "SoC n/a" : `SoC ${socPercent.toFixed(0)}%`;
  }
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
  return "kW";
}

function getAxisTitleSubRate() {
  return "W";
}

function buildRateHoverTemplate(label) {
  return `<b>${label}</b><br>%{x}<br>%{y:.3f} kW<br>%{customdata:.1f} W<extra></extra>`;
}

function buildPowerHoverTemplate(label) {
  return `<b>${label}</b><br>%{x}<br>%{y:.3f} kW<br>%{customdata:.1f} W<extra></extra>`;
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

function getBydPowerWatts(item) {
  const payload = item && item.raw_payload ? item.raw_payload : {};
  const realtime = payload && payload.realtime ? payload.realtime : {};
  const vehicle = payload && payload.vehicle ? payload.vehicle : {};
  const nestedGl = realtime.gl;
  const flatGl = payload.gl_w;
  const flatTotalPower = payload.total_power_w;
  const vehicleGl = vehicle.gl;
  const vehicleTotalPower = vehicle.totalPower;
  const normalizeNumber = (value) => {
    if (value === null || value === undefined) {
      return null;
    }
    const normalized = typeof value === "string" ? value.trim().replace(/,/g, "") : value;
    const numeric = Number(normalized);
    return Number.isFinite(numeric) ? numeric : null;
  };
  const vehicleSpeedKph = normalizeNumber(
    payload.vehicle_speed_kph ??
    realtime.speed ??
    realtime.speedKmH ??
    realtime.speedKmh ??
    realtime.vehicleSpeed ??
    vehicle.speed
  );
  if (vehicleSpeedKph !== null && vehicleSpeedKph > 0) {
    return 0;
  }
  const candidates = [
    payload.tracked_power_w,
    flatGl,
    nestedGl,
    vehicleGl,
    payload.power_w,
    flatTotalPower,
    vehicleTotalPower
  ];

  for (const candidate of candidates) {
    const numeric = normalizeNumber(candidate);
    if (numeric !== null) {
      return Math.max(0, numeric);
    }
  }
  if (item && item.grid_usage_watts !== null && item.grid_usage_watts !== undefined && !Number.isNaN(Number(item.grid_usage_watts))) {
    return Math.max(0, Number(item.grid_usage_watts) * 60);
  }
  return null;
}

function getBydVehicleSpeedKph(item) {
  const payload = item && item.raw_payload ? item.raw_payload : {};
  const realtime = payload && payload.realtime ? payload.realtime : {};
  const vehicle = payload && payload.vehicle ? payload.vehicle : {};
  const candidate = payload.vehicle_speed_kph
    ?? realtime.speed
    ?? realtime.speedKmH
    ?? realtime.speedKmh
    ?? realtime.vehicleSpeed
    ?? vehicle.speed;
  const numeric = Number(candidate);
  return Number.isFinite(numeric) ? numeric : null;
}

function getBydChargingRate(item) {
  const payload = item && item.raw_payload ? item.raw_payload : {};
  const vehicleSpeedKph = getBydVehicleSpeedKph(item);
  if (Number.isFinite(vehicleSpeedKph) && vehicleSpeedKph > 0) {
    return 0;
  }
  const candidates = [
    payload.ev_charging_rate_w_per_min,
    item && item.grid_usage_watts,
    payload.tracked_power_w !== null && payload.tracked_power_w !== undefined ? Number(payload.tracked_power_w) / 60 : null,
    payload.power_w !== null && payload.power_w !== undefined ? Math.max(0, Number(payload.power_w)) / 60 : null
  ];

  for (const candidate of candidates) {
    if (candidate !== null && candidate !== undefined && !Number.isNaN(Number(candidate))) {
      return Math.max(0, Number(candidate));
    }
  }
  return null;
}

function getBydPowerSeries(items) {
  const movementWindowMs = 2 * 60 * 1000;
  const bydItems = items
    .filter((item) => item.source === "byd_ev")
    .sort((left, right) => new Date(left.observed_at) - new Date(right.observed_at));
  const movingEpochs = bydItems
    .filter((item) => {
      const speedKph = getBydVehicleSpeedKph(item);
      return speedKph !== null && speedKph > 0;
    })
    .map((item) => new Date(item.observed_at).getTime())
    .filter((timestamp) => Number.isFinite(timestamp));

  return bydItems
    .map((item) => ({
      ...item,
      movement_suppressed: movingEpochs.some((movingEpoch) => Math.abs(new Date(item.observed_at).getTime() - movingEpoch) <= movementWindowMs),
      power_w: getBydPowerWatts(item),
      charging_rate_w_per_min: getBydChargingRate(item)
    }))
    .map((item) => item.movement_suppressed
      ? { ...item, power_w: 0, charging_rate_w_per_min: 0 }
      : item)
    .filter((item) => item.charging_rate_w_per_min !== null)
}

function getChartHeight() {
  return 270;
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

function getCollectorChipClass(state) {
  if (state === "connected") {
    return "is-connected";
  }
  if (state === "error" || state === "disconnected") {
    return "is-error";
  }
  return "is-waiting";
}

function formatCollectorLabel(name) {
  return String(name || "")
    .replace(/_/g, " ")
    .replace(/\b\w/g, (char) => char.toUpperCase());
}

function renderCollectorStrip(items) {
  if (!collectorStrip) {
    return;
  }
  collectorStrip.innerHTML = (items || [])
    .map((item) => `
      <div class="collector-chip ${getCollectorChipClass(item.state || "")}" title="${formatCollectorLabel(item.name)}: ${item.state || "unknown"}">
        <span class="collector-chip-light" aria-hidden="true"></span>
        <span>${formatCollectorLabel(item.name)}</span>
      </div>
    `)
    .join("");
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

function resetStoredChartState() {
  updateUiState((state) => {
    state.charts = {};
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
    const chargingRate = getBydChargingRate(item);
    const etaMinutes = payload.time_to_full_minutes;
    const etaText = etaMinutes === null || etaMinutes === undefined
      ? "n/a"
      : `${Math.floor(Number(etaMinutes) / 60)}h ${Number(etaMinutes) % 60}m`;
    return `
      <article class="metric-card">
        <span>byd_ev</span>
        ${formatMetricReading("BYD EV", chargingRate)}
        <small>SoC: ${payload.soc_percent === null || payload.soc_percent === undefined ? "n/a" : `${Number(payload.soc_percent).toFixed(0)}%`}</small>
        <small>Range: ${payload.range_km === null || payload.range_km === undefined ? "n/a" : `${Number(payload.range_km).toFixed(0)} km`}</small>
        <small>Charge ETA: ${etaText}</small>
        <small>Power source: ${payload.power_source || "n/a"}</small>
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

function toAppDate(dateLike) {
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

function sortByObservedAt(items) {
  return [...items].sort((left, right) => new Date(left.observed_at) - new Date(right.observed_at));
}

function buildWindowState(start, end) {
  return {
    start: new Date(start),
    end: new Date(end)
  };
}

function clipIntervalToWindow(startDate, endDate, windowState) {
  const clippedStart = startDate < windowState.start ? windowState.start : startDate;
  const clippedEnd = endDate > windowState.end ? windowState.end : endDate;
  if (clippedEnd <= clippedStart) {
    return null;
  }
  return { start: clippedStart, end: clippedEnd };
}

function getStartOfNextDay(dateLike) {
  const date = new Date(dateLike);
  date.setHours(24, 0, 0, 0);
  return date;
}

function getStartOfHour(dateLike) {
  const date = new Date(dateLike);
  date.setMinutes(0, 0, 0);
  return date;
}

function getStartOfNextHour(dateLike) {
  const date = getStartOfHour(dateLike);
  date.setHours(date.getHours() + 1);
  return date;
}

function getStartOfWeek(dateLike) {
  const date = new Date(dateLike);
  date.setHours(0, 0, 0, 0);
  const weekday = (date.getDay() + 6) % 7;
  date.setDate(date.getDate() - weekday);
  return date;
}

function getStartOfNextWeek(dateLike) {
  const date = getStartOfWeek(dateLike);
  date.setDate(date.getDate() + 7);
  return date;
}

function getStartOfNextMonth(dateLike) {
  const date = new Date(dateLike);
  return new Date(date.getFullYear(), date.getMonth() + 1, 1, 0, 0, 0, 0);
}

function getTrailingWindow(referenceDate, durationMs) {
  return {
    start: new Date(referenceDate.getTime() - durationMs),
    end: new Date(referenceDate)
  };
}

function formatHourBucketLabel(bucketKey) {
  const [datePart, timePart] = String(bucketKey).split(" ");
  const [year, month, day] = datePart.split("-").map(Number);
  const hour = Number((timePart || "00:00").split(":")[0] || 0);
  return new Date(year, month - 1, day, hour, 0, 0, 0).toLocaleTimeString("en-AU", {
    hour: "2-digit",
    minute: "2-digit"
  });
}

function formatDayBucketLabel(bucketKey) {
  const [year, month, day] = String(bucketKey).split("-").map(Number);
  return new Date(year, month - 1, day, 0, 0, 0, 0).toLocaleDateString("en-AU", {
    day: "2-digit",
    month: "short"
  });
}

function formatWeekBucketLabel(bucketKey) {
  const [year, month, day] = String(bucketKey).split("-").map(Number);
  const labelDate = new Date(year, month - 1, day, 0, 0, 0, 0);
  return `Week of ${labelDate.toLocaleDateString("en-AU", { day: "2-digit", month: "short" })}`;
}

function splitEnergyAcrossBuckets(startDate, endDate, averageRate, keyBuilder, nextBoundaryBuilder) {
  const segments = [];
  let cursor = new Date(startDate);

  while (cursor < endDate) {
    const nextBoundary = nextBoundaryBuilder(cursor);
    const segmentEnd = nextBoundary < endDate ? nextBoundary : endDate;
    const deltaMinutes = (segmentEnd - cursor) / 60000;
    if (deltaMinutes > 0) {
      segments.push({
        bucketKey: keyBuilder(cursor),
        observed_at: segmentEnd.toISOString(),
        energy_kwh: ratePerMinuteToKwh(averageRate, deltaMinutes)
      });
    }
    cursor = segmentEnd;
  }

  return segments;
}

function buildEnergySegments(
  series,
  valueKey,
  keyBuilder = getDayKey,
  nextBoundaryBuilder = getStartOfNextDay,
  windowState = null
) {
  const segments = [];
  for (let index = 1; index < series.length; index += 1) {
    const previous = series[index - 1];
    const current = series[index];
    const previousDate = toAppDate(previous.observed_at);
    const currentDate = toAppDate(current.observed_at);
    const clippedInterval = windowState
      ? clipIntervalToWindow(previousDate, currentDate, windowState)
      : { start: previousDate, end: currentDate };
    if (!clippedInterval) {
      continue;
    }
    const averageRate = (Number(previous[valueKey]) + Number(current[valueKey])) / 2;
    segments.push(
      ...splitEnergyAcrossBuckets(
        clippedInterval.start,
        clippedInterval.end,
        averageRate,
        keyBuilder,
        nextBoundaryBuilder
      )
    );
  }
  return segments;
}

function buildEnergyTotals(
  series,
  valueKey,
  keyBuilder = getDayKey,
  nextBoundaryBuilder = getStartOfNextDay,
  windowState = null
) {
  const totals = new Map();
  buildEnergySegments(series, valueKey, keyBuilder, nextBoundaryBuilder, windowState).forEach((segment) => {
    totals.set(segment.bucketKey, (totals.get(segment.bucketKey) || 0) + Number(segment.energy_kwh || 0));
  });
  return totals;
}

function integrateSeriesKwh(series, valueKey, windowState = null) {
  let cumulative = 0;
  const points = [];
  let currentDayKey = null;

  for (let index = 0; index < series.length; index += 1) {
    const current = series[index];
    const currentDate = toAppDate(current.observed_at);
    if (windowState && (currentDate < windowState.start || currentDate > windowState.end)) {
      continue;
    }
    const dayKey = getDayKey(currentDate);

    if (dayKey !== currentDayKey) {
      cumulative = 0;
      currentDayKey = dayKey;
    }

    if (index > 0) {
      const previous = series[index - 1];
      const previousDate = toAppDate(previous.observed_at);
      const clippedInterval = windowState
        ? clipIntervalToWindow(previousDate, currentDate, windowState)
        : { start: previousDate, end: currentDate };
      if (clippedInterval) {
        const averageRate = (Number(previous[valueKey]) + Number(current[valueKey])) / 2;
        const dayEnergy = splitEnergyAcrossBuckets(clippedInterval.start, clippedInterval.end, averageRate, getDayKey, getStartOfNextDay)
          .filter((segment) => segment.bucketKey === dayKey)
          .reduce((sum, segment) => sum + segment.energy_kwh, 0);
        cumulative += dayEnergy;
      }
    }

    points.push({
      observed_at: current.observed_at,
      cumulative_kwh: cumulative
    });
  }

  return points;
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
  const blePollers = (pollers || []).filter((item) => item && ["ble", "network_ble"].includes(item.name));
  const preferredPoller = blePollers.find((item) => item.state === "connected")
    || blePollers.find((item) => item.state === "starting")
    || blePollers.find((item) => item.name === "ble")
    || blePollers[0];

  const batteryPercent = preferredPoller && preferredPoller.details ? preferredPoller.details.battery_percent : null;
  const numericBatteryPercent = Number(batteryPercent);
  if (!Number.isFinite(numericBatteryPercent)) {
    return null;
  }
  return Math.max(0, Math.min(100, numericBatteryPercent));
}

function renderBleBatteryState(pollers) {
  const batteryPercent = getBleBatteryPercent(pollers);
  if (bleBatteryFill) {
    bleBatteryFill.style.width = batteryPercent === null ? "0%" : `${batteryPercent}%`;
  }
  batteryText.textContent = batteryPercent === null ? "Battery n/a" : `Battery ${batteryPercent}%`;
}

function getBydSocPercent(samples, pollers) {
  const bydSample = (samples || []).find((item) => item.source === "byd_ev");
  const samplePayload = bydSample && bydSample.raw_payload ? bydSample.raw_payload : {};
  const bydPoller = (pollers || []).find((item) => item.name === "byd_ev");
  const pollerDetails = bydPoller && bydPoller.details ? bydPoller.details : {};
  const candidate = samplePayload.soc_percent ?? pollerDetails.soc_percent;
  const numeric = Number(candidate);
  return Number.isFinite(numeric) ? Math.max(0, Math.min(100, numeric)) : null;
}

function renderEvBatteryState(samples, pollers) {
  const socPercent = getBydSocPercent(samples, pollers);
  if (evBatteryFill) {
    evBatteryFill.style.width = socPercent === null ? "0%" : `${socPercent}%`;
  }
  if (evBatteryLabel) {
    evBatteryLabel.textContent = socPercent === null ? "EV SoC n/a" : `EV SoC ${socPercent.toFixed(0)}%`;
  }
}

function getTodayAndWeekTotals(items) {
  const now = new Date();
  const todayKey = getDayKey(now);
  const weekKey = getWeekKey(now);
  const monthKey = getMonthKey(now);
  const bydItems = getBydPowerSeries(items);

  const solarSeries = items
    .filter((item) => item.source === "local_site" && item.solar_generation_watts !== null)
    .sort((left, right) => new Date(left.observed_at) - new Date(right.observed_at));
  const gridSeries = items
    .filter((item) => item.source === "ble" && item.grid_usage_watts !== null)
    .sort((left, right) => new Date(left.observed_at) - new Date(right.observed_at));

  const solarDailyTotals = buildEnergyTotals(
    solarSeries,
    "solar_generation_watts",
    getDayKey,
    getStartOfNextDay
  );
  const gridDailyTotals = buildEnergyTotals(
    gridSeries,
    "grid_usage_watts",
    getDayKey,
    getStartOfNextDay
  );
  const evDailyTotals = buildEnergyTotals(
    bydItems,
    "charging_rate_w_per_min",
    getDayKey,
    getStartOfNextDay
  );
  const solarWeeklyTotals = buildEnergyTotals(
    solarSeries,
    "solar_generation_watts",
    getWeekKey,
    getStartOfNextWeek
  );
  const gridWeeklyTotals = buildEnergyTotals(
    gridSeries,
    "grid_usage_watts",
    getWeekKey,
    getStartOfNextWeek
  );
  const evWeeklyTotals = buildEnergyTotals(
    bydItems,
    "charging_rate_w_per_min",
    getWeekKey,
    getStartOfNextWeek
  );
  const solarMonthlyTotals = buildEnergyTotals(
    solarSeries,
    "solar_generation_watts",
    getMonthKey,
    getStartOfNextMonth
  );
  const gridMonthlyTotals = buildEnergyTotals(
    gridSeries,
    "grid_usage_watts",
    getMonthKey,
    getStartOfNextMonth
  );
  const evMonthlyTotals = buildEnergyTotals(
    bydItems,
    "charging_rate_w_per_min",
    getMonthKey,
    getStartOfNextMonth
  );

  const dailySolar = solarDailyTotals.get(todayKey) || 0;
  const dailyGrid = gridDailyTotals.get(todayKey) || 0;
  const dailyEv = evDailyTotals.get(todayKey) || 0;
  const weeklySolar = solarWeeklyTotals.get(weekKey) || 0;
  const weeklyGrid = gridWeeklyTotals.get(weekKey) || 0;
  const weeklyEv = evWeeklyTotals.get(weekKey) || 0;
  const monthlySolar = solarMonthlyTotals.get(monthKey) || 0;
  const monthlyGrid = gridMonthlyTotals.get(monthKey) || 0;
  const monthlyEv = evMonthlyTotals.get(monthKey) || 0;

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

function getSeriesBySource(items) {
  return {
    solar: sortByObservedAt(items.filter((item) => item.source === "local_site" && item.solar_generation_watts !== null)),
    grid: sortByObservedAt(items.filter((item) => item.source === "ble" && item.grid_usage_watts !== null)),
    ev: getBydPowerSeries(items)
  };
}

function buildSummaryData(items, windowState) {
  const series = getSeriesBySource(items);
  const referenceEnd = new Date(windowState.end);
  const hourlyWindow = getTrailingWindow(referenceEnd, 24 * 3600000);
  const dailyWindow = getTrailingWindow(referenceEnd, 7 * 24 * 3600000);
  const weeklyWindow = getTrailingWindow(referenceEnd, 30 * 24 * 3600000);
  return {
    series,
    cumulative: {
      solar: integrateSeriesKwh(series.solar, "solar_generation_watts", windowState),
      grid: integrateSeriesKwh(series.grid, "grid_usage_watts", windowState),
      ev: integrateSeriesKwh(series.ev, "charging_rate_w_per_min", windowState)
    },
    generation: {
      hourly: buildEnergyTotals(series.solar, "solar_generation_watts", getHourKey, getStartOfNextHour, hourlyWindow),
      daily: buildEnergyTotals(series.solar, "solar_generation_watts", getDayKey, getStartOfNextDay, dailyWindow),
      weekly: buildEnergyTotals(series.solar, "solar_generation_watts", getWeekKey, getStartOfNextWeek, weeklyWindow)
    }
  };
}


const lightweightCharts = new Map();

function getChartInstance(element) {
  return lightweightCharts.get(element) || null;
}

function clearChartElement(element) {
  const existing = getChartInstance(element);
  if (existing) {
    existing.destroy();
    lightweightCharts.delete(element);
  }
  if (element) {
    element.innerHTML = "";
  }
}

function ensureChartCanvas(element) {
  clearChartElement(element);
  const canvas = document.createElement("canvas");
  canvas.setAttribute("aria-label", element.id || "chart");
  element.appendChild(canvas);
  return canvas;
}

function buildCanvasTheme() {
  const dark = getTheme() === "dark";
  return {
    dark,
    text: dark ? "#edf4ff" : "#263445",
    muted: dark ? "#97abc5" : "#7a8797",
    grid: dark ? "rgba(124, 147, 180, 0.14)" : "rgba(164, 179, 201, 0.16)",
    gridStrong: dark ? "rgba(124, 147, 180, 0.28)" : "rgba(164, 179, 201, 0.28)",
    panel: dark ? "#132134" : "#ffffff"
  };
}

function formatChartTimeLabel(epochMs) {
  const date = new Date(epochMs);
  return date.toLocaleTimeString("en-AU", {
    timeZone: appTimezone,
    hour: "2-digit",
    minute: "2-digit"
  });
}

function formatChartDateTimeLabel(epochMs) {
  const date = new Date(epochMs);
  return date.toLocaleString("en-AU", {
    timeZone: appTimezone,
    day: "2-digit",
    month: "2-digit",
    hour: "2-digit",
    minute: "2-digit"
  });
}

function baseChartOptions(theme) {
  return {
    responsive: true,
    maintainAspectRatio: false,
    animation: false,
    normalized: true,
    parsing: false,
    interaction: {
      mode: "nearest",
      intersect: false
    },
    elements: {
      point: {
        radius: 0,
        hitRadius: 8,
        hoverRadius: 3
      },
      line: {
        tension: 0
      }
    },
    plugins: {
      legend: {
        labels: {
          color: theme.muted,
          boxWidth: 10,
          boxHeight: 10,
          usePointStyle: false
        }
      },
      tooltip: {
        backgroundColor: theme.dark ? "rgba(13, 22, 35, 0.96)" : "rgba(255,255,255,0.95)",
        borderColor: theme.dark ? "rgba(135, 156, 186, 0.22)" : "rgba(120, 132, 155, 0.22)",
        borderWidth: 1,
        titleColor: theme.text,
        bodyColor: theme.text
      }
    }
  };
}

function createLineChart(element, datasets, tooltipMode = "rate") {
  if (typeof Chart === "undefined") {
    renderChartPlaceholder(element, "Chart library unavailable");
    return;
  }
  const theme = buildCanvasTheme();
  const canvas = ensureChartCanvas(element);
  const chart = new Chart(canvas, {
    type: "line",
    data: { datasets },
    options: {
      ...baseChartOptions(theme),
      interaction: {
        mode: "index",
        intersect: false
      },
      scales: {
        x: {
          type: "linear",
          grid: {
            color: theme.grid
          },
          ticks: {
            color: theme.muted,
            maxTicksLimit: 7,
            callback: (value) => formatChartTimeLabel(value)
          }
        },
        y: {
          beginAtZero: true,
          grid: {
            color: theme.grid
          },
          ticks: {
            color: theme.muted
          },
          title: {
            display: true,
            color: theme.muted,
            text: tooltipMode === "energy" ? "kWh" : "kW"
          }
        }
      },
      plugins: {
        ...baseChartOptions(theme).plugins,
        tooltip: {
          ...baseChartOptions(theme).plugins.tooltip,
          callbacks: {
            title: (items) => {
              const point = items && items[0] ? items[0].raw : null;
              return point ? formatChartDateTimeLabel(point.x) : "";
            },
            label: (context) => {
              if (tooltipMode === "energy") {
                return `${context.dataset.label}: ${Number(context.raw.y || 0).toFixed(3)} kWh`;
              }
              const rawRate = context.raw.raw;
              return `${context.dataset.label}: ${Number(context.raw.y || 0).toFixed(3)} kW (${Number(rawRate || 0).toFixed(1)} W)`;
            }
          }
        }
      }
    }
  });
  lightweightCharts.set(element, chart);
}

function createBarChart(element, labels, datasets) {
  if (typeof Chart === "undefined") {
    renderChartPlaceholder(element, "Chart library unavailable");
    return;
  }
  if (!labels.length) {
    renderChartPlaceholder(element, "No bar data in the selected window");
    return;
  }
  const theme = buildCanvasTheme();
  const canvas = ensureChartCanvas(element);
  const chart = new Chart(canvas, {
    type: "bar",
    data: { labels, datasets },
    options: {
      ...baseChartOptions(theme),
      scales: {
        x: {
          type: "category",
          stacked: false,
          grid: {
            color: "rgba(0,0,0,0)"
          },
          ticks: {
            color: theme.muted,
            maxRotation: 0,
            autoSkip: true,
            maxTicksLimit: 8
          }
        },
        y: {
          beginAtZero: true,
          grid: {
            color: theme.grid
          },
          ticks: {
            color: theme.muted
          },
          title: {
            display: true,
            color: theme.muted,
            text: "kWh"
          }
        }
      },
      plugins: {
        ...baseChartOptions(theme).plugins,
        tooltip: {
          ...baseChartOptions(theme).plugins.tooltip,
          callbacks: {
            label: (context) => `${context.dataset.label}: ${Number(context.raw || 0).toFixed(3)} kWh`
          }
        }
      }
    }
  });
  lightweightCharts.set(element, chart);
}

function renderChartPlaceholder(element, message) {
  clearChartElement(element);
  element.innerHTML = `<div class="chart-empty-state">${message}</div>`;
}

function resizeCharts() {
  lightweightCharts.forEach((chart) => chart.resize());
}

function renderDashboardCharts(items, windowState) {
  try {
    const summaryData = buildSummaryData(items, windowState);
    renderBleSolarChart(items);
    renderCumulativeChart(summaryData);
    renderGenerationSummaryCharts(summaryData);
    return true;
  } catch (error) {
    console.error("Chart render failed", error);
    renderEmptyCharts();
    return false;
  }
}

function renderBleSolarChart(items) {
  const dark = getTheme() === "dark";
  const bleGrid = items
    .filter((item) => item.source === "ble" && item.grid_usage_watts !== null)
    .sort((left, right) => new Date(left.observed_at) - new Date(right.observed_at));
  const siteSolar = items
    .filter((item) => item.source === "local_site" && item.solar_generation_watts !== null)
    .sort((left, right) => new Date(left.observed_at) - new Date(right.observed_at));
  const evItems = getBydPowerSeries(items);

  createLineChart(bleChartElement, [
    {
      label: "BLE grid",
      data: bleGrid.map((item) => ({
        x: toChartTime(item.observed_at).getTime(),
        y: ratePerMinuteToKwPerHour(item.grid_usage_watts),
        raw: Number(item.grid_usage_watts)
      })),
      borderColor: dark ? "#7fb0ff" : "#6f96d8",
      backgroundColor: dark ? "rgba(127, 176, 255, 0.16)" : "rgba(111, 150, 216, 0.17)",
      fill: true
    },
    {
      label: "Site solar",
      data: siteSolar.map((item) => ({
        x: toChartTime(item.observed_at).getTime(),
        y: ratePerMinuteToKwPerHour(item.solar_generation_watts),
        raw: Number(item.solar_generation_watts)
      })),
      borderColor: dark ? "#8ee29d" : "#7cc98a",
      backgroundColor: dark ? "rgba(142, 226, 157, 0.12)" : "rgba(124, 201, 138, 0.12)",
      fill: true
    },
    {
      label: "BYD EV",
      data: evItems.map((item) => ({
        x: toChartTime(item.observed_at).getTime(),
        y: ratePerMinuteToKwPerHour(item.charging_rate_w_per_min),
        raw: Number(item.charging_rate_w_per_min)
      })),
      borderColor: dark ? "#ffb45b" : "#d6882e",
      backgroundColor: "rgba(0,0,0,0)",
      fill: false
    }
  ], "rate");
}

function renderCumulativeChart(summaryData) {
  const dark = getTheme() === "dark";
  const solarKwh = summaryData.cumulative.solar;
  const gridKwh = summaryData.cumulative.grid;
  const evKwh = summaryData.cumulative.ev;

  createLineChart(cumulativeChartElement, [
    {
      label: "Site solar cumulative",
      data: solarKwh.map((item) => ({
        x: toChartTime(item.observed_at).getTime(),
        y: item.cumulative_kwh
      })),
      borderColor: dark ? "#8ee29d" : "#7cc98a",
      backgroundColor: dark ? "rgba(142, 226, 157, 0.14)" : "rgba(124, 201, 138, 0.12)",
      fill: true
    },
    {
      label: "BLE grid cumulative",
      data: gridKwh.map((item) => ({
        x: toChartTime(item.observed_at).getTime(),
        y: item.cumulative_kwh
      })),
      borderColor: dark ? "#7fb0ff" : "#6f96d8",
      backgroundColor: dark ? "rgba(127, 176, 255, 0.12)" : "rgba(111, 150, 216, 0.09)",
      fill: true
    },
    {
      label: "BYD EV cumulative",
      data: evKwh.map((item) => ({
        x: toChartTime(item.observed_at).getTime(),
        y: item.cumulative_kwh
      })),
      borderColor: dark ? "#ffb45b" : "#d6882e",
      backgroundColor: dark ? "rgba(255, 180, 91, 0.12)" : "rgba(214, 136, 46, 0.10)",
      fill: true
    }
  ], "energy");
}

function renderGenerationSummaryChart(element, totals, formatter) {
  const dark = getTheme() === "dark";
  const entries = Array.from(totals.entries()).sort((left, right) => left[0].localeCompare(right[0]));
  createBarChart(element, entries.map(([label]) => formatter(label)), [
    {
      label: "Site solar",
      data: entries.map(([, value]) => Number(value || 0)),
      backgroundColor: dark ? "#8ee29d" : "#7cc98a",
      borderRadius: 4,
      barPercentage: 0.88,
      categoryPercentage: 0.74
    }
  ]);
}

function renderGenerationSummaryCharts(summaryData) {
  renderGenerationSummaryChart(hourlyChartElement, summaryData.generation.hourly, formatHourBucketLabel);
  renderGenerationSummaryChart(dailyChartElement, summaryData.generation.daily, formatDayBucketLabel);
  renderGenerationSummaryChart(weeklyChartElement, summaryData.generation.weekly, formatWeekBucketLabel);
}

function renderEmptyCharts() {
  renderChartPlaceholder(bleChartElement, "No data in the selected window");
  renderChartPlaceholder(cumulativeChartElement, "No cumulative data in the selected window");
  renderChartPlaceholder(hourlyChartElement, "No hourly generation data available");
  renderChartPlaceholder(dailyChartElement, "No daily generation data available");
  renderChartPlaceholder(weeklyChartElement, "No weekly generation data available");
}

async function refresh() {
  try {
    const hours = syncWindowControls(hoursInput.value || window.SOLAR_MONITOR_CONFIG.defaultHours || 24);
    let safeStart;
    if (isFixedRange()) {
      ensureStartInputs();
      const selectedDate = startDateInput.value;
      const selectedTime = startTimeInput.value || "00:00";
      const start = buildAppDateTime(selectedDate, selectedTime);
      safeStart = Number.isNaN(start.getTime()) ? getDefaultStartDateTime(hours) : start;
    } else {
      safeStart = getDefaultStartDateTime(hours);
      syncDisplayedStart(safeStart);
      persistDateTimeControls();
    }
    const end = new Date(safeStart.getTime() + hours * 3600000);
    const windowState = buildWindowState(safeStart, end);
    const [statusResponse, samplesResponse] = await Promise.all([
      fetch("/api/status"),
      fetch(`/api/samples?hours=${hours}&start=${encodeURIComponent(safeStart.toISOString())}&end=${encodeURIComponent(end.toISOString())}`)
    ]);

    if (!statusResponse.ok || !samplesResponse.ok) {
      throw new Error(`HTTP ${statusResponse.status}/${samplesResponse.status}`);
    }

    const statusPayload = await statusResponse.json();
    const samplesPayload = await samplesResponse.json();
    const items = Array.isArray(samplesPayload.items) ? samplesPayload.items : [];

    renderStatusCards(statusPayload.pollers);
    renderCollectorStrip(statusPayload.pollers);
    latestValues.innerHTML = statusPayload.latest_samples
      .filter((item) => item.source !== "tuya_ev")
      .map(formatMetricCard)
      .join("");
    renderTopbarGauge(statusPayload.latest_samples);
    renderBydTopbarGauge(statusPayload.latest_samples, statusPayload.pollers);
    renderBleBatteryState(statusPayload.pollers);
    renderEvBatteryState(statusPayload.latest_samples, statusPayload.pollers);

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
    const chartsRendered = renderDashboardCharts(items, windowState);
    refreshText.textContent = chartsRendered
      ? `Updated ${new Date().toLocaleTimeString("en-AU", { timeZone: appTimezone })}`
      : "Updated with chart fallback";
  } catch (error) {
    console.error("Refresh failed", error);
    renderEmptyCharts();
    refreshText.textContent = "Refresh failed";
    renderCollectorStrip([]);
    renderTopbarGauge([]);
    renderBydTopbarGauge([], []);
    renderBleBatteryState([]);
    renderEvBatteryState([], []);
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
if (isFixedRange() && uiState.controls && uiState.controls.startDate) {
  startDateInput.value = uiState.controls.startDate;
}
if (isFixedRange() && uiState.controls && uiState.controls.startTime) {
  startTimeInput.value = uiState.controls.startTime;
}
syncWindowControls((uiState.controls && uiState.controls.hours) || pageLoadDefaultHours);
if (isFixedRange()) {
  ensureStartInputs();
  persistDateTimeControls();
} else {
  applyDefaultStartDateTime(hoursInput.value || pageLoadDefaultHours);
}
bindStatusCardPersistence();

themeToggle.addEventListener("click", () => {
  const nextTheme = getTheme() === "dark" ? "light" : "dark";
  setTheme(nextTheme);
  scheduleRefresh(0);
});

windowPreset.addEventListener("change", () => {
  if (windowPreset.value !== "custom") {
    syncWindowControls(windowPreset.value);
    if (!isFixedRange()) {
      applyDefaultStartDateTime(windowPreset.value);
    }
    scheduleRefresh(0);
  }
});

hoursInput.addEventListener("input", () => {
  syncWindowControls(hoursInput.value);
  if (!isFixedRange()) {
    applyDefaultStartDateTime(hoursInput.value);
  }
  scheduleRefresh(200);
});
hoursInput.addEventListener("change", () => {
  syncWindowControls(hoursInput.value);
  if (!isFixedRange()) {
    applyDefaultStartDateTime(hoursInput.value);
  }
  scheduleRefresh(0);
});

[startDateInput, startTimeInput].forEach((input) => {
  input.addEventListener("input", () => {
    setRangeMode("fixed");
    persistDateTimeControls();
    scheduleRefresh(200);
  });
  input.addEventListener("change", () => {
    setRangeMode("fixed");
    persistDateTimeControls();
    scheduleRefresh(0);
  });
});

resetRangeButton.addEventListener("click", () => {
  setRangeMode("live");
  applyDefaultStartDateTime(hoursInput.value || window.SOLAR_MONITOR_CONFIG.defaultHours || 24);
  resetStoredChartState();
  scheduleRefresh(0);
});

window.addEventListener("resize", resizeCharts);
if ("serviceWorker" in navigator) {
  const swUrl = window.SOLAR_PWA && window.SOLAR_PWA.swUrl ? window.SOLAR_PWA.swUrl : "/sw.js";
  window.addEventListener("load", () => {
    navigator.serviceWorker.register(swUrl).catch((error) => {
      console.warn("Service worker registration failed", error);
    });
  });
}
refresh();
setInterval(refresh, 30000);

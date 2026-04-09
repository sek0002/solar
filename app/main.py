from __future__ import annotations

import html
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional
from urllib.parse import urlencode
from zoneinfo import ZoneInfo

from fastapi import FastAPI, Header, HTTPException, Query, Request
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from app.config import settings
from app.database import Database
from app.pollers import PollingCoordinator


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

BASE_DIR = Path(__file__).resolve().parent.parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))
database = Database(settings.database_path)
coordinator = PollingCoordinator(settings, database)


def _static_asset_version(path: str) -> str:
    asset_path = BASE_DIR / path
    try:
        return str(int(asset_path.stat().st_mtime))
    except OSError:
        return "1"


def _parse_api_datetime(value: Optional[str], fallback: datetime) -> datetime:
    if not value:
        return fallback
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return fallback
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def _with_network_ble_placeholder(statuses: list[dict[str, object]]) -> list[dict[str, object]]:
    if settings.ble_enabled:
        return statuses
    if any(item.get("name") in {"ble", "network_ble"} for item in statuses):
        return statuses
    return [
        {
            "name": "network_ble",
            "state": "waiting",
            "last_success_at": None,
            "last_error_at": None,
            "last_error": None,
            "details": {
                "mode": "remote",
                "message": "Waiting for BLE samples/status from a remote poller",
            },
        },
        *statuses,
    ]


def _format_byd_page_value(value: object, suffix: str = "") -> str:
    if value is None or value == "":
        return "-"
    if isinstance(value, float):
        return f"{value:.1f}{suffix}"
    return f"{value}{suffix}"


def _format_eta_value(minutes: object) -> str:
    if minutes is None or minutes == "":
        return "-"
    try:
        total = int(float(minutes))
    except (TypeError, ValueError):
        return str(minutes)
    hours = total // 60
    remainder = total % 60
    return f"{hours}h {remainder}m"


def _extract_gps_coordinates(payload: object) -> tuple[float | None, float | None]:
    if not isinstance(payload, dict):
        return None, None

    lat_keys = ("latitude", "lat", "gpsLat", "gpsLatitude")
    lon_keys = ("longitude", "lon", "lng", "gpsLng", "gpsLongitude")

    def _as_float(value: object) -> float | None:
        try:
            return float(value) if value not in (None, "") else None
        except (TypeError, ValueError):
            return None

    latitude = next((_as_float(payload.get(key)) for key in lat_keys if _as_float(payload.get(key)) is not None), None)
    longitude = next((_as_float(payload.get(key)) for key in lon_keys if _as_float(payload.get(key)) is not None), None)
    if latitude is not None and longitude is not None:
        return latitude, longitude

    for value in payload.values():
        nested_latitude, nested_longitude = _extract_gps_coordinates(value)
        if nested_latitude is not None and nested_longitude is not None:
            return nested_latitude, nested_longitude

    return None, None


def _build_map_embed_url(latitude: float | None, longitude: float | None) -> str | None:
    if latitude is None or longitude is None:
        return None
    delta = 0.015
    params = urlencode(
        {
            "bbox": f"{longitude - delta:.6f},{latitude - delta:.6f},{longitude + delta:.6f},{latitude + delta:.6f}",
            "layer": "mapnik",
            "marker": f"{latitude:.6f},{longitude:.6f}",
        }
    )
    return f"https://www.openstreetmap.org/export/embed.html?{params}"


def _split_energy_across_hours(
    start: datetime,
    end: datetime,
    average_power_w: float,
    timezone_name: str,
) -> list[tuple[datetime, float]]:
    if end <= start:
        return []

    tz = ZoneInfo(timezone_name)
    current = start.astimezone(tz)
    finish = end.astimezone(tz)
    segments: list[tuple[datetime, float]] = []

    while current < finish:
        next_boundary = (current.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1))
        segment_end = min(finish, next_boundary)
        duration_hours = (segment_end - current).total_seconds() / 3600.0
        if duration_hours > 0:
            segments.append((current.replace(minute=0, second=0, microsecond=0), (average_power_w * duration_hours) / 1000.0))
        current = segment_end

    return segments


def _build_byd_daily_bar_state(samples: list[dict[str, object]], timezone_name: str) -> tuple[list[tuple[str, float]], float]:
    if len(samples) < 2:
        return [], 0.0

    hourly_totals: dict[datetime, float] = {}
    for previous, current in zip(samples, samples[1:]):
        try:
            previous_at = datetime.fromisoformat(str(previous["observed_at"]).replace("Z", "+00:00"))
            current_at = datetime.fromisoformat(str(current["observed_at"]).replace("Z", "+00:00"))
        except (KeyError, TypeError, ValueError):
            continue

        previous_payload = previous.get("raw_payload") if isinstance(previous, dict) else {}
        current_payload = current.get("raw_payload") if isinstance(current, dict) else {}
        previous_power = (
            (previous_payload or {}).get("tracked_power_w")
            or (previous_payload or {}).get("gl_w")
            or 0.0
        )
        current_power = (
            (current_payload or {}).get("tracked_power_w")
            or (current_payload or {}).get("gl_w")
            or 0.0
        )
        try:
            average_power_w = (float(previous_power) + float(current_power)) / 2.0
        except (TypeError, ValueError):
            continue

        for bucket_start, energy_kwh in _split_energy_across_hours(previous_at, current_at, average_power_w, timezone_name):
            hourly_totals[bucket_start] = hourly_totals.get(bucket_start, 0.0) + energy_kwh

    cumulative = 0.0
    bars: list[tuple[str, float]] = []
    for bucket_start in sorted(hourly_totals):
        cumulative += hourly_totals[bucket_start]
        bars.append((bucket_start.strftime("%H:%M"), cumulative))
    return bars, cumulative


def _build_byd_page(
    statuses: list[dict[str, object]], latest_samples: list[dict[str, object]], *, compact: bool = False
) -> str:
    status = next((item for item in statuses if item.get("name") == "byd_ev"), None)
    sample = next((item for item in latest_samples if item.get("source") == "byd_ev"), None)
    details = dict(status.get("details") or {}) if status else {}
    raw_payload = dict(sample.get("raw_payload") or {}) if sample else {}

    def pick(key: str) -> object:
        if key in raw_payload and raw_payload.get(key) not in (None, ""):
            return raw_payload.get(key)
        return details.get(key)

    observed_at = sample.get("observed_at") if sample else None
    state = status.get("state") if status else "missing"
    last_error = status.get("last_error") if status else None
    last_success = status.get("last_success_at") if status else None
    timezone_name = settings.timezone_name
    now_local = datetime.now(ZoneInfo(timezone_name))
    start_of_day_local = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
    byd_day_samples = database.get_samples_range(
        since=start_of_day_local.astimezone(timezone.utc),
        until=now_local.astimezone(timezone.utc),
        limit=settings.api_max_points,
    )
    byd_day_samples = [item for item in byd_day_samples if item.get("source") == "byd_ev"]
    cumulative_bars, total_kwh = _build_byd_daily_bar_state(byd_day_samples, timezone_name)
    max_bar_value = max((value for _, value in cumulative_bars), default=0.0)
    latitude, longitude = _extract_gps_coordinates(raw_payload.get("gps") or {})
    map_url = _build_map_embed_url(latitude, longitude)
    current_gl_w = raw_payload.get("tracked_power_w", raw_payload.get("gl_w"))

    bar_html = ""
    if cumulative_bars:
        bar_html = "".join(
            """
            <div class="daily-bar">
              <div class="daily-bar-fill" style="height: {height:.2f}%"></div>
              <span class="daily-bar-label">{label}</span>
              <span class="daily-bar-value">{value:.2f} kWh</span>
            </div>
            """.format(
                height=(value / max_bar_value * 100.0) if max_bar_value > 0 else 0.0,
                label=html.escape(label),
                value=value,
            )
            for label, value in cumulative_bars
        )
    else:
        bar_html = '<div class="empty-state">Waiting for enough BYD samples to build today\'s cumulative bars.</div>'

    if map_url:
        map_html = """
        <div class="map-shell">
          <iframe
            src="{map_url}"
            title="BYD vehicle location"
            loading="lazy"
            referrerpolicy="no-referrer-when-downgrade"
          ></iframe>
        </div>
        <div class="map-caption">GPS {latitude:.5f}, {longitude:.5f}</div>
        """.format(map_url=html.escape(map_url, quote=True), latitude=latitude, longitude=longitude)
    else:
        map_html = '<div class="empty-state">GPS location is not available in the latest BYD payload yet.</div>'

    overview_html = """
    <section class="overview-card">
      <div class="overview-heading">
        <div>
          <h2>Daily BYD net usage</h2>
          <p>Cumulative total built from tracked non-negative `gl` power across today.</p>
        </div>
        <div class="overview-total">
          <span>Total today</span>
          <strong>{total_kwh:.2f} kWh</strong>
          <small>Current gl {current_gl}</small>
        </div>
      </div>
      <div class="daily-bars">
        {bar_html}
      </div>
      <div class="overview-map">
        <div class="overview-map-header">
          <strong>Vehicle location</strong>
          <span>{observed_at}</span>
        </div>
        {map_html}
      </div>
    </section>
    """.format(
        total_kwh=total_kwh,
        current_gl=html.escape(_format_byd_page_value(current_gl_w, " W/hr")),
        bar_html=bar_html,
        observed_at=html.escape(_format_byd_page_value(observed_at)),
        map_html=map_html,
    )

    compact_cards = [
        ("State", _format_byd_page_value(state)),
        ("SoC", _format_byd_page_value(pick("soc_percent"), "%")),
        ("Range", _format_byd_page_value(pick("range_km"), " km")),
        ("Current gl", _format_byd_page_value(current_gl_w, " W/hr")),
        ("Power src", _format_byd_page_value(pick("power_source"))),
        ("Charging", _format_byd_page_value(pick("is_charging"))),
        ("Connected", _format_byd_page_value(pick("is_connected"))),
        ("Charge state", _format_byd_page_value(pick("charging_state"))),
        ("Charge ETA", _format_eta_value(pick("time_to_full_minutes"))),
        ("Mileage", _format_byd_page_value(pick("total_mileage_km"), " km")),
        ("Model", _format_byd_page_value(pick("model_name"))),
        ("Brand", _format_byd_page_value(pick("brand_name"))),
        ("VIN", _format_byd_page_value(pick("vin"))),
        ("Vehicle", _format_byd_page_value(pick("vehicle_state"))),
        ("Connect", _format_byd_page_value(pick("connect_state"))),
        ("Online", _format_byd_page_value(pick("online_state"))),
        ("Inside", _format_byd_page_value(pick("inside_temp_c"), " C")),
        ("Outside", _format_byd_page_value(pick("outside_temp_c"), " C")),
        ("GPS Lat", _format_byd_page_value(latitude)),
        ("GPS Lon", _format_byd_page_value(longitude)),
        ("Observed", _format_byd_page_value(observed_at)),
        ("Last success", _format_byd_page_value(last_success)),
        ("Realtime", _format_byd_page_value(pick("realtime_timestamp"))),
        ("Charge upd", _format_byd_page_value(pick("charging_update_time"))),
        ("Timezone", _format_byd_page_value(timezone_name)),
    ]

    if compact:
        cards = compact_cards
    else:
        cards = [
            ("VIN", _format_byd_page_value(pick("vin"))),
            ("Model", _format_byd_page_value(pick("model_name"))),
            ("SoC", _format_byd_page_value(pick("soc_percent"), "%")),
            ("Range", _format_byd_page_value(pick("range_km"), " km")),
            ("Power", _format_byd_page_value(pick("power_w"), " W")),
            ("Mileage", _format_byd_page_value(pick("total_mileage_km"), " km")),
            ("Inside temp", _format_byd_page_value(pick("inside_temp_c"), " C")),
            ("Outside temp", _format_byd_page_value(pick("outside_temp_c"), " C")),
            ("Charge ETA", _format_eta_value(pick("time_to_full_minutes"))),
            ("Observed at", _format_byd_page_value(observed_at)),
            ("Last success", _format_byd_page_value(last_success)),
        ]

    card_html = "".join(
        f"""
        <article class="metric">
          <div class="metric-label">{html.escape(label)}</div>
          <div class="metric-value">{html.escape(str(value))}</div>
        </article>
        """
        for label, value in cards
    )

    compact_table_html = "".join(
        f"""
        <article class="compact-cell">
          <span class="compact-cell-label">{html.escape(str(label))}</span>
          <strong class="compact-cell-value">{html.escape(str(value))}</strong>
        </article>
        """
        for label, value in compact_cards
    )

    error_html = ""
    if last_error and not compact:
        error_html = f"""
        <section class="error-box">
          <strong>Last error</strong>
          <pre>{html.escape(str(last_error))}</pre>
        </section>
        """

    raw_json = html.escape(str(raw_payload))
    nav_html = ""
    heading_html = ""
    details_html = ""
    main_class = "compact-main" if compact else ""
    body_class = "compact-body" if compact else ""
    grid_class = "grid compact-grid" if compact else "grid"
    subtitle_text = "Self-contained BYD page generated from the latest stored poller sample and status."

    if compact:
        heading_html = f"""
    <section class="compact-header">
      <div>
        <h1>BYD vehicle status</h1>
        <p>{html.escape(str(_format_byd_page_value(pick("vin"))))}</p>
      </div>
      <div class="compact-state">{html.escape(str(_format_byd_page_value(state)))}</div>
    </section>
        """
    else:
        nav_html = """
    <nav class="page-tabs" aria-label="Pages">
      <a class="page-tab" href="/">Dashboard</a>
      <a class="page-tab is-active" href="/byd">BYD</a>
    </nav>
        """
        heading_html = f"""
    <h1>BYD Vehicle Status</h1>
    <p class="subtitle">{subtitle_text}</p>
        """
        details_html = f"""
    <details>
      <summary>Raw payload</summary>
      <pre>{raw_json}</pre>
    </details>
        """

    content_html = f"""
    <section class="compact-table">
      {compact_table_html}
    </section>
    """ if compact else f"""
    {overview_html}
    <section class="{grid_class}">
      {card_html}
    </section>
    {error_html}
    {details_html}
    """

    refresh_meta = '<meta http-equiv="refresh" content="60">' if compact else ""

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  {refresh_meta}
  <title>BYD Status</title>
  <style>
    :root {{
      color-scheme: light dark;
      --bg: #0f172a;
      --panel: #111827;
      --card: #1f2937;
      --text: #e5e7eb;
      --muted: #94a3b8;
      --accent: #38bdf8;
      --error: #fecaca;
      --error-bg: #7f1d1d;
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }}
    body {{
      margin: 0;
      background: linear-gradient(180deg, #0f172a 0%, #111827 100%);
      color: var(--text);
    }}
    body.compact-body {{
      background: transparent;
      overflow: hidden;
    }}
    main {{
      max-width: 1100px;
      margin: 0 auto;
      padding: 32px 20px 48px;
    }}
    .compact-main {{
      max-width: none;
      padding: 12px;
    }}
    .page-tabs {{
      display: inline-flex;
      gap: 8px;
      margin-bottom: 18px;
      padding: 8px;
      background: rgba(17, 24, 39, 0.88);
      border: 1px solid rgba(148, 163, 184, 0.22);
      border-radius: 18px;
      box-shadow: 0 10px 24px rgba(0, 0, 0, 0.18);
    }}
    .page-tab {{
      padding: 10px 16px;
      border-radius: 12px;
      color: var(--muted);
      text-decoration: none;
      font-weight: 700;
    }}
    .page-tab.is-active {{
      color: var(--text);
      background: rgba(56, 189, 248, 0.16);
    }}
    h1 {{
      margin: 0 0 8px;
      font-size: 2rem;
    }}
    .compact-header {{
      display: flex;
      align-items: flex-start;
      justify-content: space-between;
      gap: 12px;
      margin-bottom: 12px;
    }}
    .compact-header h1 {{
      margin: 0;
      font-size: 1.1rem;
    }}
    .compact-header p {{
      margin: 4px 0 0;
      color: var(--muted);
      font-size: 0.78rem;
      word-break: break-word;
    }}
    .compact-state {{
      padding: 6px 10px;
      border-radius: 999px;
      background: rgba(56, 189, 248, 0.16);
      color: var(--text);
      font-size: 0.78rem;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: 0.04em;
      white-space: nowrap;
    }}
    .subtitle {{
      margin: 0 0 24px;
      color: var(--muted);
    }}
    h2 {{
      margin: 0;
      font-size: 1rem;
    }}
    .grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
      gap: 14px;
    }}
    .compact-grid {{
      grid-template-columns: repeat(auto-fit, minmax(120px, 1fr));
      gap: 8px;
      align-items: stretch;
    }}
    .overview-card {{
      display: grid;
      gap: 16px;
      background: rgba(17, 24, 39, 0.9);
      border: 1px solid rgba(148, 163, 184, 0.22);
      border-radius: 18px;
      padding: 16px;
      box-shadow: 0 10px 24px rgba(0, 0, 0, 0.18);
      margin-bottom: 16px;
    }}
    .compact-table {{
      display: grid;
      grid-template-columns: repeat(5, minmax(0, 1fr));
      gap: 8px;
    }}
    .compact-cell {{
      display: grid;
      gap: 4px;
      min-width: 0;
      padding: 9px 10px;
      border-radius: 12px;
      background: rgba(17, 24, 39, 0.9);
      border: 1px solid rgba(148, 163, 184, 0.18);
    }}
    .compact-cell-label {{
      color: var(--muted);
      font-size: 0.66rem;
      text-transform: uppercase;
      letter-spacing: 0.05em;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
    }}
    .compact-cell-value {{
      font-size: 0.78rem;
      line-height: 1.15;
      overflow-wrap: anywhere;
      word-break: break-word;
    }}
    .overview-heading {{
      display: flex;
      align-items: flex-start;
      justify-content: space-between;
      gap: 12px;
      flex-wrap: wrap;
    }}
    .overview-heading p {{
      margin: 6px 0 0;
      color: var(--muted);
      font-size: 0.86rem;
    }}
    .overview-total {{
      display: grid;
      justify-items: end;
      gap: 4px;
      min-width: 0;
    }}
    .overview-total span,
    .overview-total small,
    .overview-map-header span,
    .map-caption {{
      color: var(--muted);
      font-size: 0.8rem;
    }}
    .overview-total strong {{
      font-size: 1.5rem;
    }}
    .daily-bars {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(28px, 1fr));
      gap: 8px;
      align-items: end;
      min-height: 180px;
      padding: 12px;
      border-radius: 14px;
      background: rgba(15, 23, 42, 0.72);
      border: 1px solid rgba(148, 163, 184, 0.14);
    }}
    .daily-bar {{
      display: grid;
      align-content: end;
      justify-items: center;
      gap: 6px;
      min-width: 0;
      min-height: 156px;
    }}
    .daily-bar-fill {{
      width: 100%;
      min-height: 6px;
      border-radius: 999px 999px 8px 8px;
      background: linear-gradient(180deg, rgba(56, 189, 248, 0.95) 0%, rgba(96, 165, 250, 0.58) 100%);
      box-shadow: 0 8px 22px rgba(56, 189, 248, 0.22);
    }}
    .daily-bar-label,
    .daily-bar-value {{
      writing-mode: vertical-rl;
      transform: rotate(180deg);
      font-size: 0.7rem;
      color: var(--muted);
      line-height: 1;
      white-space: nowrap;
    }}
    .daily-bar-value {{
      color: var(--text);
    }}
    .overview-map {{
      display: grid;
      gap: 10px;
    }}
    .overview-map-header {{
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 10px;
      flex-wrap: wrap;
    }}
    .map-shell {{
      overflow: hidden;
      border-radius: 16px;
      border: 1px solid rgba(148, 163, 184, 0.18);
      background: rgba(15, 23, 42, 0.72);
      min-height: 220px;
    }}
    .map-shell iframe {{
      width: 100%;
      height: 220px;
      border: 0;
      display: block;
    }}
    .empty-state {{
      display: grid;
      place-items: center;
      min-height: 160px;
      padding: 16px;
      border-radius: 14px;
      border: 1px dashed rgba(148, 163, 184, 0.26);
      color: var(--muted);
      text-align: center;
    }}
    .metric {{
      background: rgba(17, 24, 39, 0.88);
      border: 1px solid rgba(148, 163, 184, 0.22);
      border-radius: 16px;
      padding: 16px;
      box-shadow: 0 10px 24px rgba(0, 0, 0, 0.18);
    }}
    .compact-grid .metric {{
      padding: 10px 12px;
      border-radius: 14px;
      box-shadow: none;
      min-width: 0;
      overflow: hidden;
    }}
    .metric-label {{
      font-size: 0.82rem;
      color: var(--muted);
      margin-bottom: 8px;
      text-transform: uppercase;
      letter-spacing: 0.04em;
    }}
    .metric-value {{
      font-size: 1.15rem;
      font-weight: 700;
      word-break: break-word;
    }}
    .compact-grid .metric-label {{
      font-size: 0.72rem;
      margin-bottom: 4px;
    }}
    .compact-grid .metric-value {{
      font-size: 0.9rem;
      line-height: 1.2;
      overflow-wrap: anywhere;
      word-break: break-word;
    }}
    @media (max-width: 720px) {{
      .compact-header {{
        flex-wrap: wrap;
      }}
      .compact-state {{
        white-space: normal;
      }}
      .compact-grid {{
        grid-template-columns: repeat(2, minmax(0, 1fr));
      }}
      .compact-table {{
        grid-template-columns: repeat(3, minmax(0, 1fr));
      }}
      .daily-bars {{
        grid-template-columns: repeat(auto-fit, minmax(22px, 1fr));
        gap: 6px;
      }}
      .daily-bar {{
        min-height: 140px;
      }}
      .map-shell,
      .map-shell iframe {{
        min-height: 190px;
        height: 190px;
      }}
    }}
    .error-box {{
      margin-top: 24px;
      padding: 16px;
      border-radius: 16px;
      background: rgba(127, 29, 29, 0.92);
      color: var(--error);
      border: 1px solid rgba(248, 113, 113, 0.35);
    }}
    pre {{
      white-space: pre-wrap;
      word-break: break-word;
      margin: 10px 0 0;
      font-size: 0.92rem;
      line-height: 1.45;
    }}
    details {{
      margin-top: 24px;
      background: rgba(17, 24, 39, 0.82);
      border: 1px solid rgba(148, 163, 184, 0.2);
      border-radius: 16px;
      padding: 16px;
    }}
    summary {{
      cursor: pointer;
      color: var(--accent);
      font-weight: 700;
    }}
  </style>
</head>
<body class="{body_class}">
  <main class="{main_class}">
    {nav_html}
    {heading_html}
    {content_html}
  </main>
</body>
</html>"""


@asynccontextmanager
async def lifespan(_: FastAPI):
    await coordinator.start()
    try:
        yield
    finally:
        await coordinator.stop()


app = FastAPI(title=settings.app_title, lifespan=lifespan)
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")


@app.get("/manifest.webmanifest")
async def manifest() -> FileResponse:
    return FileResponse(
        BASE_DIR / "static" / "manifest.webmanifest",
        media_type="application/manifest+json",
    )


@app.get("/sw.js")
async def service_worker() -> FileResponse:
    return FileResponse(
        BASE_DIR / "static" / "sw.js",
        media_type="application/javascript",
        headers={"Service-Worker-Allowed": "/"},
    )


@app.get("/", response_class=HTMLResponse)
async def index(request: Request) -> HTMLResponse:
    latest_samples = database.get_latest_samples()
    statuses = _with_network_ble_placeholder(await coordinator.statuses.snapshot())
    return templates.TemplateResponse(
        request,
        "index.html",
        {
            "app_title": settings.app_title,
            "request": request,
            "default_hours": settings.api_default_hours,
            "timezone_name": settings.timezone_name,
            "static_app_version": _static_asset_version("static/app.js"),
            "manifest_version": _static_asset_version("static/manifest.webmanifest"),
            "styles_version": _static_asset_version("static/styles.css"),
            "sw_version": _static_asset_version("static/sw.js"),
            "latest_samples": latest_samples,
            "statuses": statuses,
        },
    )


@app.get("/byd", response_class=HTMLResponse)
async def byd_page(embed: bool = Query(default=False)) -> HTMLResponse:
    latest_samples = database.get_latest_samples()
    statuses = _with_network_ble_placeholder(await coordinator.statuses.snapshot())
    return HTMLResponse(_build_byd_page(statuses, latest_samples, compact=embed))


@app.get("/api/samples")
async def api_samples(
    hours: int = Query(default=settings.api_default_hours, ge=1, le=24 * 30),
    limit: int = Query(default=settings.api_max_points, ge=1, le=20000),
    start: Optional[str] = Query(default=None),
    end: Optional[str] = Query(default=None),
) -> dict[str, object]:
    if start or end:
        fallback_end = datetime.now(timezone.utc)
        end_dt = _parse_api_datetime(end, fallback_end)
        start_dt = _parse_api_datetime(start, end_dt - timedelta(hours=hours))
        if start_dt > end_dt:
            start_dt, end_dt = end_dt - timedelta(hours=hours), end_dt
        return {"items": database.get_samples_range(since=start_dt, until=end_dt, limit=limit)}

    return {"items": database.get_recent_samples(hours=hours, limit=limit)}


@app.get("/api/status")
async def api_status() -> dict[str, object]:
    return {
        "pollers": _with_network_ble_placeholder(await coordinator.statuses.snapshot()),
        "latest_samples": database.get_latest_samples(),
    }


def _check_ingest_token(token: Optional[str]) -> None:
    configured = settings.ingest_token.strip()
    if not configured:
        return
    if token != configured:
        raise HTTPException(status_code=401, detail="Invalid ingest token")


@app.post("/api/ingest/sample")
async def api_ingest_sample(
    payload: dict[str, object],
    x_ingest_token: Optional[str] = Header(default=None),
) -> dict[str, str]:
    _check_ingest_token(x_ingest_token)
    observed_at = _parse_api_datetime(
        str(payload.get("observed_at")) if payload.get("observed_at") is not None else None,
        datetime.now(timezone.utc),
    )
    database.insert_sample(
        source=str(payload.get("source") or "unknown"),
        observed_at=observed_at,
        grid_usage_watts=float(payload["grid_usage_watts"]) if payload.get("grid_usage_watts") is not None else None,
        solar_generation_watts=float(payload["solar_generation_watts"]) if payload.get("solar_generation_watts") is not None else None,
        raw_payload=payload.get("raw_payload") if isinstance(payload.get("raw_payload"), dict) else None,
    )
    return {"status": "ok"}


@app.post("/api/ingest/status")
async def api_ingest_status(
    payload: dict[str, object],
    x_ingest_token: Optional[str] = Header(default=None),
) -> dict[str, str]:
    _check_ingest_token(x_ingest_token)
    await coordinator.statuses.update(
        str(payload.get("name") or "unknown"),
        state=str(payload["state"]) if payload.get("state") is not None else None,
        error=str(payload["error"]) if payload.get("error") is not None else None,
        details=payload.get("details") if isinstance(payload.get("details"), dict) else None,
        mark_success=bool(payload.get("mark_success")),
    )
    return {"status": "ok"}

from __future__ import annotations

import asyncio
import html
import logging
import re
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional
from urllib.parse import urlencode

import httpx
from fastapi import FastAPI, Header, HTTPException, Query, Request
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from app.config import settings
from app.database import Database
from app.pollers import PollingCoordinator, TuyaCloudClient


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

BASE_DIR = Path(__file__).resolve().parent.parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))
database = Database(settings.database_path, settings.timezone_name)
coordinator = PollingCoordinator(settings, database)
tuya_client = TuyaCloudClient(settings)
tuya_command_lock = asyncio.Lock()


def _tuya_status_map(status_payload: list[dict[str, object]]) -> dict[str, object]:
    return {
        str(item.get("code")): item.get("value")
        for item in status_payload
        if isinstance(item, dict) and item.get("code") is not None
    }


def _tuya_is_on(status_map: dict[str, object]) -> bool | None:
    work_state = str(status_map.get("work_state") or "")
    if work_state in {"charger_charging", "charger_wait"}:
        return True
    if work_state in {"charge_end", "charger_free"}:
        return False
    switch_value = status_map.get("switch")
    return switch_value if isinstance(switch_value, bool) else None


async def _tuya_get_status_map(client: httpx.AsyncClient) -> dict[str, object]:
    return _tuya_status_map(await tuya_client.get_device_status(client))


async def _tuya_wait_for_state(
    client: httpx.AsyncClient,
    *,
    desired_on: bool,
    timeout_seconds: float = 10.0,
    poll_interval_seconds: float = 1.0,
) -> dict[str, object]:
    deadline = asyncio.get_running_loop().time() + timeout_seconds
    last_status: dict[str, object] = {}
    while True:
        last_status = await _tuya_get_status_map(client)
        if _tuya_is_on(last_status) is desired_on:
            return last_status
        if asyncio.get_running_loop().time() >= deadline:
            raise HTTPException(
                status_code=504,
                detail=f"Timed out waiting for charger to reach {'ON' if desired_on else 'OFF'} state",
            )
        await asyncio.sleep(poll_interval_seconds)


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

    def _coerce_number(value: object) -> float | None:
        try:
            return float(value) if value not in (None, "") else None
        except (TypeError, ValueError):
            return None

    latitude_keys = ("latitude", "lat", "gpsLat", "gpsLatitude")
    longitude_keys = ("longitude", "lon", "lng", "gpsLng", "gpsLongitude")

    for latitude_key in latitude_keys:
        latitude = _coerce_number(payload.get(latitude_key))
        if latitude is None:
            continue
        for longitude_key in longitude_keys:
            longitude = _coerce_number(payload.get(longitude_key))
            if longitude is not None:
                return latitude, longitude

    for value in payload.values():
        latitude, longitude = _extract_gps_coordinates(value)
        if latitude is not None and longitude is not None:
            return latitude, longitude

    return None, None


def _build_map_embed_url(latitude: float | None, longitude: float | None) -> str | None:
    if latitude is None or longitude is None:
        return None
    bounds_delta = 0.015
    params = urlencode(
        {
            "bbox": f"{longitude - bounds_delta:.6f},{latitude - bounds_delta:.6f},{longitude + bounds_delta:.6f},{latitude + bounds_delta:.6f}",
            "layer": "mapnik",
            "marker": f"{latitude:.6f},{longitude:.6f}",
        }
    )
    return f"https://www.openstreetmap.org/export/embed.html?{params}"


def _read_byd_re_status_html() -> Optional[str]:
    status_path = Path(settings.byd_re_dir).expanduser() / "status.html"
    if not status_path.exists():
        return None
    try:
        return status_path.read_text(encoding="utf-8")
    except OSError:
        return None


def _decorate_byd_re_status_html(status_html: str) -> str:
    dark_style = """
<style id="solar-byd-dark-override">
  :root { color-scheme: dark; }
  html, body {
    background: #0f172a !important;
    color: #e5e7eb !important;
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif !important;
  }
  body {
    margin: 0 !important;
    padding: 24px 20px 36px !important;
  }
  .solar-byd-toolbar {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 12px;
    margin: 0 auto 20px;
    max-width: 1200px;
  }
  .solar-byd-back {
    display: inline-flex;
    align-items: center;
    gap: 8px;
    padding: 10px 16px;
    border-radius: 999px;
    background: rgba(56, 189, 248, 0.16);
    color: #e5e7eb !important;
    text-decoration: none;
    font-weight: 700;
  }
  .solar-byd-back:hover {
    background: rgba(56, 189, 248, 0.26);
  }
  .solar-byd-title {
    color: #94a3b8;
    font-size: 0.9rem;
  }
  .container, main, .content, .wrapper {
    max-width: 1200px;
    margin-left: auto !important;
    margin-right: auto !important;
  }
  img, svg, canvas {
    max-width: 100% !important;
    height: auto !important;
  }
  .card, .panel, table, section, article, .metric, .tile {
    background: rgba(17, 24, 39, 0.88) !important;
    color: #e5e7eb !important;
    border-color: rgba(148, 163, 184, 0.18) !important;
  }
  h1, h2, h3, h4, h5, h6, strong, b, th, td, p, span, div, li, label {
    color: inherit;
  }
  a { color: #38bdf8 !important; }
</style>
<script id="solar-byd-hide-car">
window.addEventListener("load", () => {
  const selectorList = [
    '[data-testid*="car"]',
    '[class*="car-panel"]',
    '[class*="carPanel"]',
    '[class*="vehicle-panel"]',
    '[class*="vehiclePanel"]',
    '[class*="car-card"]',
    '[class*="vehicle-card"]',
    '#car',
    '#vehicle'
  ];

  const explicitPanel = selectorList
    .map((selector) => document.querySelector(selector))
    .find(Boolean);

  if (explicitPanel) {
    explicitPanel.remove();
  }
});
</script>
"""
    toolbar = """
<div class="solar-byd-toolbar">
  <a class="solar-byd-back" href="/">Return to dashboard</a>
  <div class="solar-byd-title">Live BYD-re status page</div>
</div>
"""
    if "solar-byd-dark-override" in status_html:
        return status_html
    if "</head>" in status_html:
        status_html = status_html.replace("</head>", f"{dark_style}</head>", 1)
    else:
        status_html = dark_style + status_html
    if "<body" in status_html:
        status_html = re.sub(r"(<body[^>]*>)", r"\1" + toolbar, status_html, count=1, flags=re.IGNORECASE)
    else:
        status_html = toolbar + status_html
    return status_html


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

    data_age_text = "-"
    if observed_at:
        try:
            observed_dt = datetime.fromisoformat(str(observed_at).replace("Z", "+00:00"))
            if observed_dt.tzinfo is None:
                observed_dt = observed_dt.replace(tzinfo=timezone.utc)
            age_seconds = max(0, int((datetime.now(timezone.utc) - observed_dt.astimezone(timezone.utc)).total_seconds()))
            if age_seconds < 60:
                data_age_text = f"{age_seconds}s"
            elif age_seconds < 3600:
                data_age_text = f"{age_seconds // 60}m"
            else:
                hours = age_seconds // 3600
                minutes = (age_seconds % 3600) // 60
                data_age_text = f"{hours}h {minutes}m"
        except ValueError:
            data_age_text = "-"

    if compact:
        gl_value = raw_payload.get("tracked_power_w")
        if gl_value in (None, ""):
            gl_value = raw_payload.get("gl_w")
        if gl_value in (None, ""):
            gl_value = pick("power_w")
        cards = [
            ("SoC", _format_byd_page_value(pick("soc_percent"), "%")),
            ("Range", _format_byd_page_value(pick("range_km"), " km")),
            ("GL", _format_byd_page_value(gl_value, " W/hr")),
            ("Charge ETA", _format_eta_value(pick("time_to_full_minutes"))),
            ("Mileage", _format_byd_page_value(pick("total_mileage_km"), " km")),
            ("Inside temp", _format_byd_page_value(pick("inside_temp_c"), " C")),
            ("Data age", data_age_text),
        ]
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

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
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
    .grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
      gap: 14px;
    }}
    .compact-grid {{
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 8px;
      align-items: stretch;
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
        grid-template-columns: repeat(3, minmax(0, 1fr));
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
    <section class="{grid_class}">
      {card_html}
    </section>
    {error_html}
    {details_html}
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
    byd_sample = next((item for item in latest_samples if item.get("source") == "byd_ev"), None)
    byd_payload = dict(byd_sample.get("raw_payload") or {}) if byd_sample else {}
    byd_latitude, byd_longitude = _extract_gps_coordinates(byd_payload.get("gps") or {})
    byd_map_embed_url = _build_map_embed_url(byd_latitude, byd_longitude)
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
            "tuya_control_enabled": bool(settings.tuya_access_id and settings.tuya_access_secret and settings.tuya_device_id),
            "latest_samples": latest_samples,
            "statuses": statuses,
            "byd_map_embed_url": byd_map_embed_url,
            "byd_map_latitude": byd_latitude,
            "byd_map_longitude": byd_longitude,
        },
    )


@app.get("/byd", response_class=HTMLResponse)
async def byd_page(embed: bool = Query(default=False)) -> HTMLResponse:
    if not embed:
        status_html = _read_byd_re_status_html()
        if status_html:
            return HTMLResponse(_decorate_byd_re_status_html(status_html))
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


@app.get("/api/cumulative")
async def api_cumulative() -> dict[str, object]:
    return {"items": database.get_cumulative_samples()}


@app.get("/api/energy-summary")
async def api_energy_summary() -> dict[str, object]:
    return database.get_energy_summary()


@app.post("/api/tuya/charger")
async def api_tuya_charger(payload: dict[str, object]) -> dict[str, object]:
    if not settings.tuya_access_id or not settings.tuya_access_secret or not settings.tuya_device_id:
        raise HTTPException(status_code=503, detail="Tuya charger control is not configured")

    enabled = payload.get("enabled")
    if not isinstance(enabled, bool):
        raise HTTPException(status_code=400, detail="Expected boolean 'enabled' field")

    async with tuya_command_lock:
        async with httpx.AsyncClient(timeout=settings.tuya_timeout_seconds) as client:
            result = await tuya_client.send_device_commands(
                client,
                [{"code": "switch", "value": enabled}],
            )
            status_map = await _tuya_wait_for_state(client, desired_on=enabled, timeout_seconds=10.0)
    return {"status": "ok", "enabled": enabled, "result": result.get("result"), "device_status": status_map}


@app.post("/api/tuya/charger/current")
async def api_tuya_charger_current(payload: dict[str, object]) -> dict[str, object]:
    if not settings.tuya_access_id or not settings.tuya_access_secret or not settings.tuya_device_id:
        raise HTTPException(status_code=503, detail="Tuya charger control is not configured")

    current = payload.get("current")
    if not isinstance(current, int) or current not in {6, 10, 13}:
        raise HTTPException(status_code=400, detail="Expected integer 'current' field with value 6, 10, or 13")

    async with tuya_command_lock:
        async with httpx.AsyncClient(timeout=settings.tuya_timeout_seconds) as client:
            initial_status = await _tuya_get_status_map(client)
            was_on = _tuya_is_on(initial_status) is True

            if was_on:
                await tuya_client.send_device_commands(
                    client,
                    [{"code": "switch", "value": False}],
                )
                await _tuya_wait_for_state(client, desired_on=False, timeout_seconds=10.0)
                await asyncio.sleep(1.0)

            result = await tuya_client.send_device_commands(
                client,
                [{"code": "charge_cur_set", "value": current}],
            )
            await asyncio.sleep(1.0)

            restored_status: dict[str, object] | None = None
            if was_on:
                await tuya_client.send_device_commands(
                    client,
                    [{"code": "switch", "value": True}],
                )
                restored_status = await _tuya_wait_for_state(client, desired_on=True, timeout_seconds=10.0)
            else:
                restored_status = await _tuya_get_status_map(client)

    return {
        "status": "ok",
        "current": current,
        "result": result.get("result"),
        "was_on": was_on,
        "device_status": restored_status,
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

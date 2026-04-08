from __future__ import annotations

import html
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, Header, HTTPException, Query, Request
from fastapi.responses import HTMLResponse
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


def _build_byd_page(statuses: list[dict[str, object]], latest_samples: list[dict[str, object]]) -> str:
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

    cards = [
        ("State", _format_byd_page_value(state)),
        ("VIN", _format_byd_page_value(pick("vin"))),
        ("Model", _format_byd_page_value(pick("model_name"))),
        ("SoC", _format_byd_page_value(pick("soc_percent"), "%")),
        ("Range", _format_byd_page_value(pick("range_km"), " km")),
        ("Charging", _format_byd_page_value(pick("charging_state"))),
        ("Connected", _format_byd_page_value(pick("is_connected"))),
        ("Charging now", _format_byd_page_value(pick("is_charging"))),
        ("Power", _format_byd_page_value(pick("power_w"), " W")),
        ("Mileage", _format_byd_page_value(pick("total_mileage_km"), " km")),
        ("Inside temp", _format_byd_page_value(pick("inside_temp_c"), " C")),
        ("Outside temp", _format_byd_page_value(pick("outside_temp_c"), " C")),
        ("ETA", _format_byd_page_value(pick("time_to_full_minutes"), " min")),
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
    if last_error:
        error_html = f"""
        <section class="error-box">
          <strong>Last error</strong>
          <pre>{html.escape(str(last_error))}</pre>
        </section>
        """

    raw_json = html.escape(str(raw_payload))
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
    main {{
      max-width: 1100px;
      margin: 0 auto;
      padding: 32px 20px 48px;
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
    .subtitle {{
      margin: 0 0 24px;
      color: var(--muted);
    }}
    .grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
      gap: 14px;
    }}
    .metric {{
      background: rgba(17, 24, 39, 0.88);
      border: 1px solid rgba(148, 163, 184, 0.22);
      border-radius: 16px;
      padding: 16px;
      box-shadow: 0 10px 24px rgba(0, 0, 0, 0.18);
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
<body>
  <main>
    <nav class="page-tabs" aria-label="Pages">
      <a class="page-tab" href="/">Dashboard</a>
      <a class="page-tab is-active" href="/byd">BYD</a>
    </nav>
    <h1>BYD Vehicle Status</h1>
    <p class="subtitle">Self-contained BYD page generated from the latest stored poller sample and status.</p>
    <section class="grid">
      {card_html}
    </section>
    {error_html}
    <details>
      <summary>Raw payload</summary>
      <pre>{raw_json}</pre>
    </details>
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
            "latest_samples": latest_samples,
            "statuses": statuses,
        },
    )


@app.get("/byd", response_class=HTMLResponse)
async def byd_page() -> HTMLResponse:
    latest_samples = database.get_latest_samples()
    statuses = _with_network_ble_placeholder(await coordinator.statuses.snapshot())
    return HTMLResponse(_build_byd_page(statuses, latest_samples))


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

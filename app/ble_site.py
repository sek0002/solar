from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import FastAPI
from fastapi.responses import HTMLResponse, PlainTextResponse

from app.config import settings
from app.database import Database
from app.pollers import PollingCoordinator


database = Database(settings.database_path)
coordinator = PollingCoordinator(settings, database)


def _latest_ble_snapshot() -> tuple[dict[str, object] | None, dict[str, object] | None]:
    latest_samples = database.get_latest_samples()
    sample = next((item for item in latest_samples if item.get("source") == "ble"), None)
    return sample, None


async def _ble_status() -> dict[str, object] | None:
    statuses = await coordinator.statuses.snapshot()
    return next((item for item in statuses if item.get("name") == "ble"), None)


def _ble_page_text(sample: dict[str, object] | None, status: dict[str, object] | None) -> str:
    raw_payload = dict(sample.get("raw_payload") or {}) if sample else {}
    details = dict(status.get("details") or {}) if status else {}
    grid_usage = sample.get("grid_usage_watts") if sample else None
    battery_percent = raw_payload.get("battery_percent", details.get("battery_percent"))
    observed_at = sample.get("observed_at") if sample else None
    state = status.get("state") if status else "missing"

    def fmt(value: object) -> str:
        if value is None or value == "":
            return ""
        if isinstance(value, float):
            return f"{value:.3f}"
        return str(value)

    return "\n".join(
        [
            fmt(grid_usage),
            fmt(battery_percent),
            fmt(observed_at),
            fmt(state),
        ]
    )


@asynccontextmanager
async def lifespan(_: FastAPI):
    await coordinator.start()
    try:
        yield
    finally:
        await coordinator.stop()


app = FastAPI(title="Powerpal BLE Site", lifespan=lifespan)


@app.get("/", response_class=PlainTextResponse)
async def ble_text_page() -> PlainTextResponse:
    sample, _ = _latest_ble_snapshot()
    status = await _ble_status()
    return PlainTextResponse(_ble_page_text(sample, status))


@app.get("/html", response_class=HTMLResponse)
async def ble_html_page() -> HTMLResponse:
    sample, _ = _latest_ble_snapshot()
    status = await _ble_status()
    body_text = _ble_page_text(sample, status)
    now = datetime.now(timezone.utc).isoformat()
    return HTMLResponse(
        f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>BLE Poller Site</title>
  <style>
    body {{
      margin: 0;
      padding: 24px;
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      background: #0f172a;
      color: #e5e7eb;
    }}
    main {{
      max-width: 720px;
      margin: 0 auto;
      padding: 20px;
      border-radius: 18px;
      background: rgba(17, 24, 39, 0.92);
      border: 1px solid rgba(148, 163, 184, 0.18);
    }}
    h1 {{
      margin-top: 0;
      font-size: 1.4rem;
    }}
    p {{
      color: #94a3b8;
    }}
    pre {{
      padding: 16px;
      border-radius: 14px;
      background: rgba(2, 6, 23, 0.88);
      color: #e5e7eb;
      overflow: auto;
    }}
  </style>
</head>
<body>
  <main>
    <h1>BLE Poller Site</h1>
    <p>Simple BLE text page for remote scraping. Updated {now}.</p>
    <pre>{body_text}</pre>
  </main>
</body>
</html>"""
    )

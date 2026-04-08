#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import Any


def _pick_first(*values: Any) -> Any:
    for value in values:
        if value is not None and value != "":
            return value
    return None


def _as_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _extract_data_from_status_html(status_html: str) -> tuple[dict[str, Any], Any]:
    match = re.search(r"var data = (.*?);\s*var generatedAt = (.*?);", status_html, re.S)
    if not match:
        raise ValueError("Unable to extract BYD data from status.html")
    data = json.loads(match.group(1))
    generated_at = json.loads(match.group(2))
    return data, generated_at


def _derive_flags(vehicle_info: dict[str, Any]) -> tuple[bool, bool]:
    charge_state = str(_pick_first(vehicle_info.get("chargingState"), vehicle_info.get("chargeState")) or "").strip().lower()
    connect_state = str(vehicle_info.get("connectState") or "").strip().lower()
    online_state = str(vehicle_info.get("onlineState") or "").strip().lower()

    is_charging = charge_state not in {"", "0", "none", "idle", "not charging", "nocharging", "stop", "stopped", "finish", "finished"}
    is_connected = connect_state not in {"", "0", "none", "offline", "disconnected"} if connect_state else online_state not in {"", "2", "offline"}
    return is_charging, is_connected


def main() -> int:
    if not os.getenv("BYD_USERNAME") or not os.getenv("BYD_PASSWORD"):
        print(json.dumps({"error": "BYD_USERNAME and BYD_PASSWORD are required"}))
        return 1

    byd_re_dir = Path(os.getenv("BYD_RE_DIR", "/opt/byd-re")).expanduser()
    node_bin = os.getenv("BYD_NODE_BIN", "node")
    client_path = byd_re_dir / "client.js"
    status_path = byd_re_dir / "status.html"

    if not client_path.exists():
        print(json.dumps({"error": f"BYD-re client not found at {client_path}"}))
        return 1

    env = os.environ.copy()
    try:
        result = subprocess.run(
            [node_bin, str(client_path)],
            cwd=str(byd_re_dir),
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )
    except FileNotFoundError:
        print(json.dumps({"error": f"Node executable not found: {node_bin}"}))
        return 1

    stdout_text = (result.stdout or "").strip()
    stderr_text = (result.stderr or "").strip()

    if result.returncode != 0:
        message = stderr_text or stdout_text or f"BYD-re client exited with code {result.returncode}"
        final_line = message.splitlines()[-1].strip()
        print(json.dumps({"error": final_line}))
        return 1

    if not status_path.exists():
        print(json.dumps({"error": f"BYD-re did not produce status.html at {status_path}"}))
        return 1

    try:
        data, generated_at = _extract_data_from_status_html(status_path.read_text(encoding="utf-8"))
    except Exception as exc:
        print(json.dumps({"error": f"Unable to parse BYD-re output: {exc}"}))
        return 1

    requested_vin = (os.getenv("BYD_VIN") or "").strip()
    vin = str(data.get("vin") or requested_vin or "")
    vehicles = data.get("vehicles") or []
    vehicle = next((item for item in vehicles if str(item.get("vin") or "") == vin), None)
    if vehicle is None and vehicles:
        vehicle = vehicles[0]

    vehicle_info = data.get("vehicleInfo") or {}
    gps_info = data.get("gps") or {}

    soc_percent = _pick_first(vehicle_info.get("elecPercent"), vehicle_info.get("powerBattery"))
    range_km = _pick_first(vehicle_info.get("enduranceMileage"), vehicle_info.get("evEndurance"))
    charge_state = _pick_first(vehicle_info.get("chargingState"), vehicle_info.get("chargeState"))
    power_w = _pick_first(vehicle_info.get("totalPower"), vehicle_info.get("gl"))
    power_w = _as_float(power_w)

    remaining_hours = _as_float(vehicle_info.get("remainingHours"))
    remaining_minutes = _as_float(vehicle_info.get("remainingMinutes"))
    time_to_full_minutes = None
    if remaining_hours is not None or remaining_minutes is not None:
        time_to_full_minutes = int((remaining_hours or 0.0) * 60 + (remaining_minutes or 0.0))

    is_charging, is_connected = _derive_flags(vehicle_info)

    payload = {
        "vin": vin or (vehicle or {}).get("vin"),
        "model_name": _pick_first((vehicle or {}).get("modelName"), vehicle_info.get("modelName"), (vehicle or {}).get("outModelType")),
        "brand_name": _pick_first((vehicle or {}).get("brandName"), vehicle_info.get("brandName")),
        "soc_percent": _as_float(soc_percent),
        "range_km": _as_float(range_km),
        "charging_state": charge_state,
        "is_charging": is_charging,
        "is_connected": is_connected,
        "time_to_full_minutes": time_to_full_minutes,
        "power_w": power_w,
        "power_source": "totalPower" if vehicle_info.get("totalPower") not in (None, "") else ("gl" if vehicle_info.get("gl") not in (None, "") else None),
        "charge_rate": _as_float(vehicle_info.get("chargeRate")),
        "total_mileage_km": _as_float(_pick_first(vehicle_info.get("totalMileageV2"), vehicle_info.get("totalMileage"))),
        "realtime_timestamp": vehicle_info.get("time"),
        "charging_update_time": generated_at,
        "inside_temp_c": _as_float(vehicle_info.get("tempInCar")),
        "outside_temp_c": _as_float(vehicle_info.get("tempOutCar")),
        "connect_state": vehicle_info.get("connectState"),
        "online_state": vehicle_info.get("onlineState"),
        "vehicle_state": vehicle_info.get("vehicleState"),
        "vehicle": vehicle or {},
        "realtime": vehicle_info,
        "charging": {
            "remainingHours": vehicle_info.get("remainingHours"),
            "remainingMinutes": vehicle_info.get("remainingMinutes"),
            "chargingState": vehicle_info.get("chargingState"),
            "chargeState": vehicle_info.get("chargeState"),
            "connectState": vehicle_info.get("connectState"),
            "onlineState": vehicle_info.get("onlineState"),
        },
        "gps": gps_info,
    }
    print(json.dumps(payload))
    return 0


if __name__ == "__main__":
    sys.exit(main())

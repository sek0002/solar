#!/usr/bin/env python3
from __future__ import annotations

import asyncio
import json
import os
import sys
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any


def _serialize(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Enum):
        return getattr(value, "name", str(value))
    return value


def _pick_first(*values: Any) -> Any:
    for value in values:
        if value is not None and value != "":
            return value
    return None


async def main() -> int:
    try:
        from pybyd import BydClient, BydConfig
    except Exception as exc:  # pragma: no cover - runtime dependency only
        print(json.dumps({"error": f"Unable to import pyBYD: {exc}"}))
        return 1

    if not os.getenv("BYD_USERNAME") or not os.getenv("BYD_PASSWORD"):
        print(json.dumps({"error": "BYD_USERNAME and BYD_PASSWORD are required"}))
        return 1

    config = BydConfig.from_env()

    async with BydClient(config) as client:
        vehicles = await client.get_vehicles()
        if not vehicles:
            print(json.dumps({"error": "No BYD vehicles found for account"}))
            return 1

        requested_vin = (os.getenv("BYD_VIN") or "").strip()
        vehicle = next((item for item in vehicles if item.vin == requested_vin), None)
        if vehicle is None:
            vehicle = vehicles[0]

        vin = vehicle.vin
        realtime = await client.get_vehicle_realtime(vin)
        charging = await client.get_charging_status(vin)

        soc_percent = _pick_first(getattr(charging, "soc", None), getattr(realtime, "elec_percent", None), getattr(realtime, "power_battery", None))
        range_km = _pick_first(getattr(realtime, "endurance_mileage", None), getattr(realtime, "ev_endurance", None))
        power_w = _pick_first(getattr(realtime, "gl", None), getattr(realtime, "total_power", None))
        power_source = None
        if getattr(realtime, "gl", None) is not None:
            power_source = "gl"
        elif getattr(realtime, "total_power", None) is not None:
            power_source = "total_power"
        elif getattr(realtime, "rate", None) is not None:
            power_source = "rate"
            power_w = float(getattr(realtime, "rate")) * 1000.0

        result = {
            "vin": vin,
            "model_name": getattr(vehicle, "model_name", None),
            "brand_name": getattr(vehicle, "brand_name", None),
            "soc_percent": soc_percent,
            "range_km": range_km,
            "charging_state": _serialize(getattr(realtime, "effective_charging_state", None)),
            "is_charging": bool(getattr(realtime, "is_charging", False) or getattr(charging, "is_charging", False)),
            "is_connected": bool(getattr(realtime, "is_charger_connected", False) or getattr(charging, "is_connected", False)),
            "time_to_full_minutes": _pick_first(getattr(charging, "time_to_full_minutes", None), getattr(realtime, "time_to_full_minutes", None)),
            "power_w": power_w,
            "power_source": power_source,
            "charge_rate": getattr(realtime, "rate", None),
            "total_mileage_km": getattr(realtime, "total_mileage", None),
            "realtime_timestamp": _serialize(getattr(realtime, "timestamp", None)),
            "charging_update_time": _serialize(getattr(charging, "update_time", None)),
            "vehicle": {
                "vin": vin,
                "model_name": getattr(vehicle, "model_name", None),
                "brand_name": getattr(vehicle, "brand_name", None),
                "auto_alias": getattr(vehicle, "auto_alias", None),
            },
            "realtime": {key: _serialize(value) for key, value in realtime.model_dump().items()},
            "charging": {key: _serialize(value) for key, value in charging.model_dump().items()},
        }
        print(json.dumps(result, default=_serialize))
        return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))

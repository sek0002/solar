#!/usr/bin/env python3
from __future__ import annotations

import asyncio
import json
import os
import secrets
import time
from typing import Any


def _patch_pybyd_login() -> None:
    import pybyd.client as client_module
    from pybyd._api import login as login_module
    from pybyd._crypto.aes import aes_encrypt_hex
    from pybyd._crypto.hashing import compute_checkcode, md5_hex, pwd_login_key, sha1_mixed
    from pybyd._crypto.signing import build_sign_string
    from pybyd._version import __version__ as pybyd_version

    app_name = getattr(login_module, "_APP_NAME", f"pyBYD+{pybyd_version}")

    def _common_outer_fields(config: Any) -> dict[str, str]:
        existing = getattr(login_module, "_common_outer_fields", None)
        if callable(existing):
            return existing(config)
        return {
            "ostype": config.device.ostype,
            "imei": config.device.imei,
            "mac": config.device.mac,
            "model": config.device.model,
            "sdk": config.device.sdk,
            "mod": config.device.mod,
        }

    def patched_build_login_request(config: Any, now_ms: int) -> dict[str, Any]:
        random_hex = secrets.token_hex(16).upper()
        req_timestamp = str(now_ms)
        service_time = str(int(time.time() * 1000))

        imei_md5 = getattr(config.device, "imei_md5", "") or ""
        if not imei_md5 or set(imei_md5) <= {"0"}:
            imei_md5 = md5_hex(config.username)

        inner: dict[str, str] = {
            "agreeStatus": "0",
            "agreementType": "[1,2]",
            "appInnerVersion": config.app_inner_version,
            "appVersion": config.app_version,
            "deviceName": f"{config.device.mobile_brand}{config.device.mobile_model}",
            "deviceType": config.device.device_type,
            "imeiMD5": imei_md5,
            "isAuto": config.is_auto,
            "mobileBrand": config.device.mobile_brand,
            "mobileModel": config.device.mobile_model,
            "networkType": config.device.network_type,
            "osType": config.device.os_type,
            "osVersion": config.device.os_version,
            "random": random_hex,
            "softType": config.soft_type,
            "timeStamp": req_timestamp,
            "timeZone": config.time_zone,
        }
        encry_data = aes_encrypt_hex(
            json.dumps(inner, separators=(",", ":")),
            pwd_login_key(config.password),
        )
        password_md5 = md5_hex(config.password)
        sign_fields: dict[str, str] = {
            **inner,
            "appName": app_name,
            "countryCode": config.country_code,
            "functionType": "pwdLogin",
            "identifier": config.username,
            "identifierType": "0",
            "language": config.language,
            "reqTimestamp": req_timestamp,
        }
        sign = sha1_mixed(build_sign_string(sign_fields, password_md5))
        outer: dict[str, Any] = {
            "appName": app_name,
            "countryCode": config.country_code,
            "encryData": encry_data,
            "functionType": "pwdLogin",
            "identifier": config.username,
            "identifierType": "0",
            "imeiMD5": imei_md5,
            "isAuto": config.is_auto,
            "language": config.language,
            "reqTimestamp": req_timestamp,
            "serviceTime": service_time,
            "sign": sign,
            "signKey": config.password,
            **_common_outer_fields(config),
        }
        outer["checkcode"] = compute_checkcode(outer)
        return outer

    login_module._APP_NAME = app_name
    login_module.build_login_request = patched_build_login_request
    client_module.build_login_request = patched_build_login_request


def _pick_first(*values: Any) -> Any:
    for value in values:
        if value not in (None, ""):
            return value
    return None


def _as_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _eta_text_from_minutes(total_minutes: Any) -> str | None:
    minutes = _as_float(total_minutes)
    if minutes is None:
        return None
    rounded = int(minutes)
    hours = rounded // 60
    remainder = rounded % 60
    return f"{hours}h {remainder}m"


async def _fetch_payload() -> dict[str, Any]:
    _patch_pybyd_login()

    from pybyd import BydClient, BydConfig

    config = BydConfig.from_env()
    async with BydClient(config) as client:
        await client.login()
        vehicles = await client.get_vehicles()
        if not vehicles:
            raise RuntimeError("pyBYD returned no vehicles")

        env_vin = (os.environ.get("BYD_VIN") or "").strip()
        vehicle = next((item for item in vehicles if str(item.vin or "") == env_vin), None)
        if vehicle is None:
            vehicle = vehicles[0]
        vin = str(vehicle.vin or env_vin or "")

        realtime = await client.get_vehicle_realtime(vin)

        gps = None
        try:
            gps = await client.get_gps_info(vin)
        except Exception:
            gps = None

        charging = None
        try:
            charging = await client.get_charging_status(vin)
        except Exception:
            charging = None

        effective_charging_state = getattr(realtime, "effective_charging_state", None)
        effective_charging_state_name = getattr(effective_charging_state, "name", None)
        realtime_charge_state = getattr(realtime, "charge_state", None)
        realtime_charge_state_name = getattr(realtime_charge_state, "name", None)
        realtime_connect_state = getattr(realtime, "connect_state", None)
        realtime_connect_state_name = getattr(realtime_connect_state, "name", None)
        realtime_online_state = getattr(realtime, "online_state", None)
        realtime_online_state_name = getattr(realtime_online_state, "name", None)

        soc_percent = _pick_first(
            getattr(realtime, "elec_percent", None),
            getattr(realtime, "power_battery", None),
            getattr(charging, "soc", None),
        )
        range_km = _pick_first(
            getattr(realtime, "endurance_mileage", None),
            getattr(realtime, "ev_endurance", None),
            getattr(realtime, "endurance_mileage_v2", None),
        )
        power_w = _pick_first(
            getattr(realtime, "gl", None),
            getattr(realtime, "total_power", None),
            getattr(realtime, "rate", None),
        )
        time_to_full_minutes = _pick_first(
            getattr(realtime, "time_to_full_minutes", None),
            getattr(charging, "time_to_full_minutes", None),
        )
        is_charging = bool(
            _pick_first(
                getattr(realtime, "is_charging", None),
                getattr(charging, "is_charging", None),
                False,
            )
        )
        is_connected = bool(
            _pick_first(
                getattr(realtime, "is_charger_connected", None),
                getattr(charging, "is_connected", None),
                False,
            )
        )

        realtime_dump = realtime.model_dump(mode="json")
        charging_dump = charging.model_dump(mode="json") if charging is not None else {}
        gps_dump = gps.model_dump(mode="json") if gps is not None else {}
        vehicle_dump = vehicle.model_dump(mode="json")

        observed_at = _pick_first(
            realtime_dump.get("timestamp"),
            charging_dump.get("update_time"),
            gps_dump.get("gps_timestamp"),
        )

        charging_state = _pick_first(
            effective_charging_state_name,
            realtime_charge_state_name,
            realtime_dump.get("charging_state"),
            charging_dump.get("charging_state"),
        )
        connect_state = _pick_first(
            realtime_connect_state_name,
            realtime_dump.get("connect_state"),
            charging_dump.get("connect_state"),
        )
        online_state = _pick_first(
            realtime_online_state_name,
            realtime_dump.get("online_state"),
        )

        return {
            "vin": vin,
            "model_name": vehicle.model_name,
            "brand_name": vehicle.brand_name,
            "soc_percent": _as_float(soc_percent),
            "range_km": _as_float(range_km),
            "vehicle_speed_kph": _as_float(_pick_first(getattr(realtime, "speed", None), getattr(gps, "speed", None))),
            "charging_state": str(charging_state) if charging_state not in (None, "") else None,
            "is_charging": bool(is_charging),
            "is_connected": bool(is_connected),
            "time_to_full_minutes": int(time_to_full_minutes) if time_to_full_minutes not in (None, "") else None,
            "time_to_full_text": _eta_text_from_minutes(time_to_full_minutes),
            "power_w": _as_float(power_w),
            "gl_w": _as_float(getattr(realtime, "gl", None)),
            "total_power_w": _as_float(getattr(realtime, "total_power", None)),
            "power_source": "gl" if getattr(realtime, "gl", None) is not None else ("totalPower" if getattr(realtime, "total_power", None) is not None else ("rate" if getattr(realtime, "rate", None) is not None else None)),
            "charge_rate": _as_float(getattr(realtime, "rate", None)),
            "total_mileage_km": _as_float(_pick_first(getattr(realtime, "total_mileage_v2", None), getattr(realtime, "total_mileage", None), getattr(vehicle, "total_mileage", None))),
            "realtime_timestamp": realtime_dump.get("timestamp"),
            "realtime_timestamp_utc": realtime_dump.get("timestamp"),
            "charging_update_time": charging_dump.get("update_time"),
            "charging_update_time_utc": charging_dump.get("update_time"),
            "observed_at": observed_at,
            "inside_temp_c": _as_float(getattr(realtime, "temp_in_car", None)),
            "outside_temp_c": _as_float(getattr(realtime, "temp_out_car", None)),
            "connect_state": connect_state,
            "online_state": online_state,
            "vehicle_state": realtime_dump.get("vehicle_state"),
            "vehicle": vehicle_dump,
            "realtime": realtime_dump,
            "charging": charging_dump,
            "gps": gps_dump,
            "raw_payload": {
                "vehicle": vehicle_dump,
                "realtime": realtime_dump,
                "charging": charging_dump,
                "gps": gps_dump,
                "effective_charging_state": effective_charging_state_name,
                "charge_state": realtime_charge_state_name,
                "connect_state": realtime_connect_state_name,
                "online_state": realtime_online_state_name,
            },
        }


def main() -> int:
    try:
        payload = asyncio.run(_fetch_payload())
    except Exception as exc:
        print(json.dumps({"error": str(exc)}))
        return 1

    print(json.dumps(payload))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

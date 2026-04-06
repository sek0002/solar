from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
import os
import re
import struct
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional
import uuid

import httpx
import pytz
from bleak import BleakClient, BleakError, BleakScanner

from app.config import Settings
from app.database import Database


LOGGER = logging.getLogger(__name__)

PAIRING_CODE_CHAR = "59da0011-12f4-25a6-7d4f-55961dce4205"
POWERPAL_FREQ_CHAR = "59da0013-12f4-25a6-7d4f-55961dce4205"
NOTIFY_CHAR = "59da0001-12f4-25a6-7d4f-55961dce4205"
BATTERY_CHAR = "00002a19-0000-1000-8000-00805f9b34fb"


@dataclass
class PollerStatus:
    name: str
    state: str = "idle"
    last_success_at: Optional[str] = None
    last_error_at: Optional[str] = None
    last_error: Optional[str] = None
    details: dict[str, Any] = field(default_factory=dict)


class StatusRegistry:
    def __init__(self) -> None:
        self._statuses: dict[str, PollerStatus] = {}
        self._lock = asyncio.Lock()

    async def update(
        self,
        name: str,
        *,
        state: Optional[str] = None,
        error: Optional[str] = None,
        details: Optional[dict[str, Any]] = None,
        mark_success: bool = False,
    ) -> None:
        async with self._lock:
            status = self._statuses.setdefault(name, PollerStatus(name=name))
            now = datetime.now(timezone.utc).isoformat()
            if state is not None:
                status.state = state
            if details is not None:
                status.details = details
            if mark_success:
                status.last_success_at = now
                status.last_error = None
            if error is not None:
                status.last_error = error
                status.last_error_at = now

    async def snapshot(self) -> list[dict[str, Any]]:
        async with self._lock:
            return [
                {
                    "name": status.name,
                    "state": status.state,
                    "last_success_at": status.last_success_at,
                    "last_error_at": status.last_error_at,
                    "last_error": status.last_error,
                    "details": status.details,
                }
                for status in self._statuses.values()
            ]


class PowerpalBlePoller:
    def __init__(self, settings: Settings, database: Database, statuses: StatusRegistry) -> None:
        self.settings = settings
        self.database = database
        self.statuses = statuses
        self._stopped = asyncio.Event()
        self._melbourne_tz = pytz.timezone(settings.timezone_name)

    @staticmethod
    def convert_pairing_code(original_pairing_code: str) -> bytes:
        return int(original_pairing_code).to_bytes(4, byteorder="little")

    async def _resolve_powerpal_device(self) -> Any:
        devices = await BleakScanner.discover(
            timeout=self.settings.ble_connection_timeout_seconds,
            return_adv=True,
        )

        exact_match = None
        name_match = None
        for _, (device, _) in devices.items():
            device_name = device.name or ""
            if device.address.lower() == self.settings.ble_mac.lower():
                exact_match = device
                break
            if "powerpal" in device_name.lower() and name_match is None:
                name_match = device

        if exact_match is not None:
            return exact_match

        if name_match is not None:
            LOGGER.warning(
                "Using Powerpal device matched by name instead of exact MAC: requested=%s resolved=%s",
                self.settings.ble_mac,
                getattr(name_match, "address", "unknown"),
            )
            return name_match

        raise BleakError(f"Could not find Powerpal device during scan for {self.settings.ble_mac}")

    def _parse_notification(self, data: bytearray) -> dict[str, Any]:
        if len(data) < 6:
            raise ValueError(f"Expected at least 6 BLE bytes, received {len(data)}")
        timestamp = struct.unpack_from("<I", data, 0)[0]
        int_array = list(data)
        pulse_sum = int_array[4] + int_array[5]
        usage_watts = pulse_sum / 0.8
        utc_time = datetime.fromtimestamp(timestamp, tz=timezone.utc)
        return {
            "observed_at": utc_time.astimezone(self._melbourne_tz),
            "grid_usage_watts": usage_watts,
            "raw_bytes_hex": data.hex(),
            "pulse_byte_4": int_array[4],
            "pulse_byte_5": int_array[5],
            "pulse_sum": pulse_sum,
            "original_test2_formula": "grid_usage_watts = (byte4 + byte5) / 0.8",
        }

    async def run(self) -> None:
        await self.statuses.update("ble", state="starting", details={"mac": self.settings.ble_mac})
        while not self._stopped.is_set():
            try:
                await self.statuses.update("ble", state="connecting", details={"mac": self.settings.ble_mac})
                await self._run_session()
                if not self._stopped.is_set():
                    await self.statuses.update(
                        "ble",
                        state="disconnected",
                        error="BLE disconnected",
                        details={"mac": self.settings.ble_mac},
                    )
                    await asyncio.sleep(self.settings.ble_retry_delay_seconds)
            except BleakError as exc:
                LOGGER.warning("BLE error: %s", exc)
                await self._record_error_fallback(str(exc))
                await self.statuses.update(
                    "ble",
                    state="error",
                    error=str(exc),
                    details={"mac": self.settings.ble_mac},
                )
                await asyncio.sleep(self.settings.ble_retry_delay_seconds)
            except Exception as exc:
                LOGGER.exception("Unexpected BLE failure")
                await self._record_error_fallback(str(exc))
                await self.statuses.update(
                    "ble",
                    state="error",
                    error=str(exc),
                    details={"mac": self.settings.ble_mac},
                )
                await asyncio.sleep(self.settings.ble_retry_delay_seconds)

    async def stop(self) -> None:
        self._stopped.set()

    async def _run_session(self) -> None:
        batch_size_bytes = int(self.settings.ble_reading_batch_size_minutes).to_bytes(4, byteorder="little")
        resolved_device = await self._resolve_powerpal_device()

        def notification_handler(_: Any, data: bytearray) -> None:
            asyncio.create_task(self._on_notification(bytearray(data)))

        async with BleakClient(resolved_device, timeout=self.settings.ble_connection_timeout_seconds) as client:
            try:
                paired = await client.pair()
                LOGGER.info("BLE pair result: %s", paired)
            except Exception as exc:
                LOGGER.debug("BLE pair step skipped or unsupported: %s", exc)

            await client.write_gatt_char(
                PAIRING_CODE_CHAR,
                self.convert_pairing_code(self.settings.ble_pairing_code),
                response=False,
            )
            await asyncio.sleep(2.0)

            current_batch_minutes = None
            battery_level = None
            try:
                batch_value = await client.read_gatt_char(POWERPAL_FREQ_CHAR)
                if len(batch_value) >= 4:
                    current_batch_minutes = int.from_bytes(batch_value[:4], byteorder="little", signed=False)
            except Exception as exc:
                LOGGER.debug("Unable to read Powerpal batch size", exc_info=exc)

            if current_batch_minutes != self.settings.ble_reading_batch_size_minutes:
                await client.write_gatt_char(
                    POWERPAL_FREQ_CHAR,
                    batch_size_bytes,
                    response=False,
                )

            try:
                battery_value = await client.read_gatt_char(BATTERY_CHAR)
                if battery_value:
                    battery_level = int(battery_value[0])
            except Exception as exc:
                LOGGER.debug("Unable to read Powerpal battery level", exc_info=exc)

            await client.start_notify(NOTIFY_CHAR, notification_handler)
            await self.statuses.update(
                "ble",
                state="connected",
                details={
                    "mac": self.settings.ble_mac,
                    "resolved_address": getattr(resolved_device, "address", self.settings.ble_mac),
                    "resolved_name": getattr(resolved_device, "name", None),
                    "configured_batch_minutes": self.settings.ble_reading_batch_size_minutes,
                    "device_batch_minutes": current_batch_minutes,
                    "battery_percent": battery_level,
                },
            )

            try:
                while not self._stopped.is_set():
                    await asyncio.sleep(1.0)
            finally:
                try:
                    await client.stop_notify(NOTIFY_CHAR)
                except Exception:
                    LOGGER.debug("Unable to stop bleak notifications cleanly", exc_info=True)

    async def _record_error_fallback(self, error_message: str) -> None:
        average_grid = self.database.get_recent_average(
            source="ble",
            column="grid_usage_watts",
            count=self.settings.failure_average_window,
        )
        if average_grid is None:
            average_grid = 0.0

        self.database.insert_sample(
            source="ble",
            observed_at=datetime.now(timezone.utc),
            grid_usage_watts=average_grid,
            solar_generation_watts=None,
            raw_payload={
                "imputed": True,
                "fallback_reason": error_message,
                "average_window": self.settings.failure_average_window,
            },
        )

    async def _on_notification(self, data: bytearray) -> None:
        sample = self._parse_notification(data)
        self.database.insert_sample(
            source="ble",
            observed_at=sample["observed_at"],
            grid_usage_watts=sample["grid_usage_watts"],
            solar_generation_watts=None,
            raw_payload=sample,
        )
        await self.statuses.update(
            "ble",
            state="connected",
            mark_success=True,
            details={
                "mac": self.settings.ble_mac,
                "battery_percent": self._extract_existing_status_detail("battery_percent"),
                "resolved_address": self._extract_existing_status_detail("resolved_address"),
                "resolved_name": self._extract_existing_status_detail("resolved_name"),
                "configured_batch_minutes": self._extract_existing_status_detail("configured_batch_minutes"),
                "device_batch_minutes": self._extract_existing_status_detail("device_batch_minutes"),
                "latest_grid_usage_watts": sample["grid_usage_watts"],
                "latest_observed_at": sample["observed_at"].isoformat(),
            },
        )

    def _extract_existing_status_detail(self, key: str) -> Any:
        statuses = getattr(self.statuses, "_statuses", {})
        status = statuses.get("ble")
        if not status:
            return None
        return status.details.get(key)


class LocalSitePoller:
    def __init__(self, settings: Settings, database: Database, statuses: StatusRegistry) -> None:
        self.settings = settings
        self.database = database
        self.statuses = statuses
        self._stopped = asyncio.Event()
        self._site_404_started_at: Optional[datetime] = None
        self._site_connection_failure_started_at: Optional[datetime] = None

    async def run(self) -> None:
        await self.statuses.update(
            "local_site",
            state="starting",
            details={"url": self.settings.local_site_url},
        )
        async with httpx.AsyncClient(timeout=self.settings.local_site_timeout_seconds) as client:
            while not self._stopped.is_set():
                try:
                    response = await client.get(self.settings.local_site_url)
                    if response.status_code == 404:
                        if self._site_404_started_at is None:
                            self._site_404_started_at = datetime.now(timezone.utc)
                        await self._record_error_fallback(
                            "HTTP 404 from {url}".format(url=self.settings.local_site_url),
                            average_window=self.settings.local_site_404_average_window,
                            zero_after_streak=self._site_404_started_at,
                        )
                        await self.statuses.update(
                            "local_site",
                            state="error",
                            error=f"HTTP 404 from {self.settings.local_site_url}",
                            details={
                                "url": self.settings.local_site_url,
                                "status_code": 404,
                                "404_streak_started_at": self._site_404_started_at.isoformat(),
                            },
                        )
                        await asyncio.sleep(self.settings.local_site_poll_seconds)
                        continue

                    response.raise_for_status()
                    self._site_404_started_at = None
                    self._site_connection_failure_started_at = None
                    payload = self._parse_response(response)
                    if (
                        payload.get("grid_usage_watts") is None
                        and payload.get("solar_generation_watts") is None
                    ):
                        raise ValueError(
                            "Response was reachable, but no grid or solar metric matched the configured JSON path or regex."
                        )
                    observed_at = datetime.now(timezone.utc)
                    self.database.insert_sample(
                        source="local_site",
                        observed_at=observed_at,
                        grid_usage_watts=payload.get("grid_usage_watts"),
                        solar_generation_watts=payload.get("solar_generation_watts"),
                        raw_payload=payload,
                    )
                    await self.statuses.update(
                        "local_site",
                        state="connected",
                        mark_success=True,
                        details={
                            "url": self.settings.local_site_url,
                            "latest_grid_usage_watts": payload.get("grid_usage_watts"),
                            "latest_solar_generation_watts": payload.get("solar_generation_watts"),
                            "latest_observed_at": observed_at.isoformat(),
                        },
                    )
                except httpx.HTTPError as exc:
                    if self._site_connection_failure_started_at is None:
                        self._site_connection_failure_started_at = datetime.now(timezone.utc)
                    await self._record_error_fallback(
                        str(exc),
                        zero_after_streak=self._site_connection_failure_started_at,
                        zero_after_minutes=self.settings.local_site_connection_zero_after_minutes,
                    )
                    await self.statuses.update(
                        "local_site",
                        state="error",
                        error=str(exc),
                        details={
                            "url": self.settings.local_site_url,
                            "connection_failure_started_at": self._site_connection_failure_started_at.isoformat(),
                        },
                    )
                except Exception as exc:
                    LOGGER.exception("Unexpected local site failure")
                    if self._site_connection_failure_started_at is None:
                        self._site_connection_failure_started_at = datetime.now(timezone.utc)
                    await self._record_error_fallback(
                        str(exc),
                        zero_after_streak=self._site_connection_failure_started_at,
                        zero_after_minutes=self.settings.local_site_connection_zero_after_minutes,
                    )
                    await self.statuses.update(
                        "local_site",
                        state="error",
                        error=str(exc),
                        details={
                            "url": self.settings.local_site_url,
                            "connection_failure_started_at": self._site_connection_failure_started_at.isoformat(),
                        },
                    )

                await asyncio.sleep(self.settings.local_site_poll_seconds)

    async def stop(self) -> None:
        self._stopped.set()

    async def _record_error_fallback(
        self,
        error_message: str,
        average_window: Optional[int] = None,
        zero_after_streak: Optional[datetime] = None,
        zero_after_minutes: Optional[float] = None,
    ) -> None:
        if not self.settings.local_site_zero_on_error:
            return

        window = average_window or self.settings.failure_average_window
        observed_at = datetime.now(timezone.utc)
        use_zero_fallback = False
        if zero_after_streak is not None:
            streak_minutes = (observed_at - zero_after_streak).total_seconds() / 60.0
            threshold_minutes = (
                zero_after_minutes
                if zero_after_minutes is not None
                else self.settings.local_site_404_zero_after_minutes
            )
            use_zero_fallback = streak_minutes >= threshold_minutes

        average_grid = self.database.get_recent_average(
            source="local_site",
            column="grid_usage_watts",
            count=window,
        )
        average_solar = self.database.get_recent_average(
            source="local_site",
            column="solar_generation_watts",
            count=window,
        )

        payload = {
            "content_type": None,
            "grid_usage_watts": 0.0 if use_zero_fallback else (average_grid if average_grid is not None else 0.0),
            "solar_generation_watts": 0.0 if use_zero_fallback else (average_solar if average_solar is not None else 0.0),
            "url": self.settings.local_site_url,
            "fallback_reason": error_message,
            "imputed": True,
            "average_window": window,
            "zero_after_minutes": (
                zero_after_minutes
                if zero_after_minutes is not None
                else self.settings.local_site_404_zero_after_minutes
            ),
            "used_zero_fallback": use_zero_fallback,
        }
        self.database.insert_sample(
            source="local_site",
            observed_at=observed_at,
            grid_usage_watts=payload["grid_usage_watts"],
            solar_generation_watts=payload["solar_generation_watts"],
            raw_payload=payload,
        )

    def _parse_response(self, response: httpx.Response) -> dict[str, Any]:
        body_text = response.text
        parsed_json: Optional[Any] = None
        if self.settings.local_site_format in {"auto", "json"}:
            try:
                parsed_json = response.json()
            except json.JSONDecodeError:
                parsed_json = None

        grid_usage = self._extract_value(
            parsed_json=parsed_json,
            body_text=body_text,
            json_path=self.settings.local_usage_json_path,
            regex=self.settings.local_usage_regex,
            line_index=self.settings.local_usage_line_index,
            divisor=self.settings.local_usage_divisor,
            multiplier=self.settings.local_usage_multiplier,
        )
        solar_generation = self._extract_value(
            parsed_json=parsed_json,
            body_text=body_text,
            json_path=self.settings.local_solar_json_path,
            regex=self.settings.local_solar_regex,
            line_index=self.settings.local_solar_line_index,
            divisor=self.settings.local_solar_divisor,
            multiplier=self.settings.local_solar_multiplier,
        )

        return {
            "content_type": response.headers.get("content-type"),
            "grid_usage_watts": grid_usage,
            "solar_generation_watts": solar_generation,
            "url": self.settings.local_site_url,
        }

    @staticmethod
    def _extract_value(
        *,
        parsed_json: Optional[Any],
        body_text: str,
        json_path: str,
        regex: str,
        line_index: Optional[int],
        divisor: float,
        multiplier: float,
    ) -> Optional[float]:
        if json_path and parsed_json is not None:
            current = parsed_json
            for part in json_path.split("."):
                if isinstance(current, dict):
                    current = current.get(part)
                elif isinstance(current, list) and part.isdigit():
                    current = current[int(part)]
                else:
                    current = None
                    break
            if current is not None:
                return (float(current) * multiplier) / divisor

        if regex:
            match = re.search(regex, body_text, flags=re.IGNORECASE | re.MULTILINE)
            if match:
                group = match.group(1) if match.groups() else match.group(0)
                return (float(group) * multiplier) / divisor

        if line_index is not None:
            lines = body_text.splitlines()
            if 0 <= line_index < len(lines):
                value = lines[line_index].strip()
                return (float(value) * multiplier) / divisor

        return None


class TuyaCloudClient:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._access_token: Optional[str] = None
        self._token_expires_at = 0.0

    def _build_headers(self, method: str, path: str, access_token: str = "") -> dict[str, str]:
        timestamp = str(int(datetime.now(timezone.utc).timestamp() * 1000))
        nonce = uuid.uuid4().hex
        content_sha256 = hashlib.sha256(b"").hexdigest()
        string_to_sign = "{method}\n{content_sha}\n\n{path}".format(
            method=method.upper(),
            content_sha=content_sha256,
            path=path,
        )
        sign_input = "{client_id}{token}{timestamp}{nonce}{string_to_sign}".format(
            client_id=self.settings.tuya_access_id,
            token=access_token,
            timestamp=timestamp,
            nonce=nonce,
            string_to_sign=string_to_sign,
        )
        signature = hmac.new(
            self.settings.tuya_access_secret.encode(),
            sign_input.encode(),
            hashlib.sha256,
        ).hexdigest().upper()
        headers = {
            "client_id": self.settings.tuya_access_id,
            "sign": signature,
            "sign_method": "HMAC-SHA256",
            "t": timestamp,
            "nonce": nonce,
        }
        if access_token:
            headers["access_token"] = access_token
        return headers

    async def get_access_token(self, client: httpx.AsyncClient) -> str:
        now = datetime.now(timezone.utc).timestamp()
        if self._access_token and now < self._token_expires_at - 60:
            return self._access_token

        path = "/v1.0/token?grant_type=1"
        response = await client.get(
            "{base}{path}".format(base=self.settings.tuya_base_url, path=path),
            headers=self._build_headers("GET", path),
        )
        response.raise_for_status()
        payload = response.json()
        if not payload.get("success"):
            raise RuntimeError("Tuya token request failed: {code} {msg}".format(
                code=payload.get("code"),
                msg=payload.get("msg"),
            ))

        result = payload.get("result") or {}
        self._access_token = result.get("access_token")
        expire_seconds = int(result.get("expire_time", 3600))
        self._token_expires_at = now + expire_seconds
        if not self._access_token:
            raise RuntimeError("Tuya token response missing access token")
        return self._access_token

    async def get_device_status(self, client: httpx.AsyncClient) -> list[dict[str, Any]]:
        token = await self.get_access_token(client)
        path = "/v1.0/devices/{device_id}/status".format(device_id=self.settings.tuya_device_id)
        response = await client.get(
            "{base}{path}".format(base=self.settings.tuya_base_url, path=path),
            headers=self._build_headers("GET", path, token),
        )
        response.raise_for_status()
        payload = response.json()
        if not payload.get("success"):
            raise RuntimeError("Tuya status request failed: {code} {msg}".format(
                code=payload.get("code"),
                msg=payload.get("msg"),
            ))
        result = payload.get("result") or []
        if not isinstance(result, list):
            raise RuntimeError("Unexpected Tuya status payload shape")
        return result


class TuyaEvPoller:
    def __init__(self, settings: Settings, database: Database, statuses: StatusRegistry) -> None:
        self.settings = settings
        self.database = database
        self.statuses = statuses
        self._stopped = asyncio.Event()
        self._client = TuyaCloudClient(settings)

    async def run(self) -> None:
        await self.statuses.update(
            "tuya_ev",
            state="starting",
            details={"device_id": self.settings.tuya_device_id or "missing"},
        )
        async with httpx.AsyncClient(timeout=self.settings.tuya_timeout_seconds) as client:
            while not self._stopped.is_set():
                try:
                    if not self.settings.tuya_access_id or not self.settings.tuya_access_secret or not self.settings.tuya_device_id:
                        raise RuntimeError("Tuya EV poller requires TUYA_ACCESS_ID, TUYA_ACCESS_SECRET, and TUYA_DEVICE_ID")

                    statuses = await self._client.get_device_status(client)
                    sample = self._parse_statuses(statuses)
                    observed_at = datetime.now(timezone.utc)
                    self.database.insert_sample(
                        source="tuya_ev",
                        observed_at=observed_at,
                        grid_usage_watts=sample["ev_charging_rate_w_per_min"],
                        solar_generation_watts=None,
                        raw_payload=sample,
                    )
                    await self.statuses.update(
                        "tuya_ev",
                        state="connected",
                        mark_success=True,
                        details={
                            "device_id": self.settings.tuya_device_id,
                            "voltage_v": sample.get("voltage_v"),
                            "current_a": sample.get("current_a"),
                            "power_kw": sample.get("power_kw"),
                            "temperature_c": sample.get("temperature_c"),
                            "session_energy_kwh": sample.get("session_energy_kwh"),
                            "latest_observed_at": observed_at.isoformat(),
                        },
                    )
                except Exception as exc:
                    LOGGER.warning("Tuya EV error: %s", exc)
                    await self.statuses.update(
                        "tuya_ev",
                        state="error",
                        error=str(exc),
                        details={"device_id": self.settings.tuya_device_id or "missing"},
                    )
                await asyncio.sleep(self.settings.tuya_poll_seconds)

    async def stop(self) -> None:
        self._stopped.set()

    def _parse_statuses(self, statuses: list[dict[str, Any]]) -> dict[str, Any]:
        by_code = {str(item.get("code")): item.get("value") for item in statuses if isinstance(item, dict)}

        def read_number(code: str, divisor: float) -> Optional[float]:
            value = by_code.get(code)
            if value is None:
                return None
            try:
                return float(value) / divisor
            except (TypeError, ValueError):
                return None

        def first_number(candidates: list[tuple[str, float]]) -> tuple[Optional[float], Optional[str]]:
            for code, divisor in candidates:
                value = read_number(code, divisor)
                if value is not None:
                    return value, code
            return None, None

        voltage_v, voltage_code = first_number([
            (self.settings.tuya_voltage_code, self.settings.tuya_voltage_divisor),
            ("volt", 10.0),
            ("cur_voltage", 10.0),
        ])
        current_a, current_code = first_number([
            (self.settings.tuya_current_code, self.settings.tuya_current_divisor),
            ("cur_current", 1000.0),
            ("current", 1000.0),
        ])
        power_kw, power_code = first_number([
            (self.settings.tuya_power_code, self.settings.tuya_power_divisor),
            ("power_total", 1000.0),
            ("sigle_phase_power", 1000.0),
            ("cur_power", 1000.0),
            ("power", 1000.0),
            ("power_kw", 1.0),
        ])
        temperature_c, temperature_code = first_number([
            (self.settings.tuya_temperature_code, self.settings.tuya_temperature_divisor),
            ("temp_current", 10.0),
            ("temperature", 10.0),
        ])
        session_energy_kwh, session_energy_code = first_number([
            (self.settings.tuya_session_energy_code, self.settings.tuya_session_energy_divisor),
            ("charge_energy_once", 100.0),
            ("forward_energy_total", 100.0),
            ("add_ele", 100.0),
            ("charge_energy", 100.0),
        ])

        if power_kw is None:
            available_codes = ", ".join(sorted(by_code.keys()))
            raise RuntimeError(
                "Tuya EV power code missing from status payload. Tried {codes}. Available codes: {available}".format(
                    codes=", ".join([self.settings.tuya_power_code, "power_total", "sigle_phase_power", "cur_power", "power", "power_kw"]),
                    available=available_codes or "none",
                )
            )

        return {
            "voltage_v": voltage_v,
            "voltage_code_used": voltage_code,
            "current_a": current_a,
            "current_code_used": current_code,
            "power_kw": power_kw,
            "power_code_used": power_code,
            "temperature_c": temperature_c,
            "temperature_code_used": temperature_code,
            "session_energy_kwh": session_energy_kwh,
            "session_energy_code_used": session_energy_code,
            "ev_charging_rate_w_per_min": (power_kw * 1000.0) / 60.0,
            "status_codes": statuses,
        }


class BydEvPoller:
    def __init__(self, settings: Settings, database: Database, statuses: StatusRegistry) -> None:
        self.settings = settings
        self.database = database
        self.statuses = statuses
        self._stopped = asyncio.Event()
        self._script_path = Path(__file__).resolve().parents[1] / "scripts" / "byd_poll.py"

    async def run(self) -> None:
        await self.statuses.update(
            "byd_ev",
            state="starting",
            details={"vin": self.settings.byd_vin or "auto"},
        )
        while not self._stopped.is_set():
            try:
                sample = await self._fetch_sample()
                observed_at = datetime.now(timezone.utc)
                self.database.insert_sample(
                    source="byd_ev",
                    observed_at=observed_at,
                    grid_usage_watts=sample["ev_charging_rate_w_per_min"],
                    solar_generation_watts=None,
                    raw_payload=sample,
                )
                await self.statuses.update(
                    "byd_ev",
                    state="connected",
                    mark_success=True,
                    details={
                        "vin": sample.get("vin") or self.settings.byd_vin or "auto",
                        "model_name": sample.get("model_name"),
                        "brand_name": sample.get("brand_name"),
                        "soc_percent": sample.get("soc_percent"),
                        "range_km": sample.get("range_km"),
                        "charging_state": sample.get("charging_state"),
                        "is_charging": sample.get("is_charging"),
                        "is_connected": sample.get("is_connected"),
                        "time_to_full_minutes": sample.get("time_to_full_minutes"),
                        "power_w": sample.get("power_w"),
                        "power_source": sample.get("power_source"),
                        "latest_observed_at": observed_at.isoformat(),
                    },
                )
            except Exception as exc:
                LOGGER.warning("BYD EV error: %s", exc)
                await self.statuses.update(
                    "byd_ev",
                    state="error",
                    error=str(exc),
                    details={"vin": self.settings.byd_vin or "auto"},
                )
            await asyncio.sleep(self.settings.byd_poll_seconds)

    async def stop(self) -> None:
        self._stopped.set()

    async def _fetch_sample(self) -> dict[str, Any]:
        env = os.environ.copy()
        process = await asyncio.create_subprocess_exec(
            self.settings.byd_python_bin,
            str(self._script_path),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=env,
        )
        try:
            stdout, stderr = await asyncio.wait_for(
                process.communicate(),
                timeout=self.settings.byd_command_timeout_seconds,
            )
        except asyncio.TimeoutError:
            process.kill()
            await process.communicate()
            raise RuntimeError("BYD poll timed out")

        stdout_text = stdout.decode().strip()
        stderr_text = stderr.decode().strip()
        if process.returncode != 0:
            message = stderr_text or stdout_text or f"BYD helper exited with code {process.returncode}"
            raise RuntimeError(message)

        try:
            payload = json.loads(stdout_text)
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"Invalid BYD helper JSON: {exc}") from exc

        if payload.get("error"):
            raise RuntimeError(str(payload["error"]))

        power_w = payload.get("power_w")
        charging_rate_w_per_min = float(power_w) / 60.0 if power_w is not None else 0.0
        payload["ev_charging_rate_w_per_min"] = charging_rate_w_per_min
        return payload


class PollingCoordinator:
    def __init__(self, settings: Settings, database: Database) -> None:
        self.settings = settings
        self.database = database
        self.statuses = StatusRegistry()
        self.tasks: list[asyncio.Task[Any]] = []
        self.pollers: list[Any] = []

    async def start(self) -> None:
        if self.settings.ble_enabled:
            ble_poller = PowerpalBlePoller(self.settings, self.database, self.statuses)
            self.pollers.append(ble_poller)
            self.tasks.append(asyncio.create_task(ble_poller.run(), name="ble-poller"))

        if self.settings.local_site_enabled:
            site_poller = LocalSitePoller(self.settings, self.database, self.statuses)
            self.pollers.append(site_poller)
            self.tasks.append(asyncio.create_task(site_poller.run(), name="local-site-poller"))

        if self.settings.byd_enabled:
            byd_poller = BydEvPoller(self.settings, self.database, self.statuses)
            self.pollers.append(byd_poller)
            self.tasks.append(asyncio.create_task(byd_poller.run(), name="byd-ev-poller"))

    async def stop(self) -> None:
        for poller in self.pollers:
            await poller.stop()
        for task in self.tasks:
            task.cancel()
        if self.tasks:
            await asyncio.gather(*self.tasks, return_exceptions=True)

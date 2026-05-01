from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
import os
import re
import struct
import sys
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
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
tuya_command_lock = asyncio.Lock()


def watts_to_rate_per_minute(value: Optional[float]) -> Optional[float]:
    if value is None:
        return None
    return float(value) / 60.0


def _coerce_optional_bool(value: object) -> Optional[bool]:
    if isinstance(value, bool):
        return value
    if value is None:
        return None
    text = str(value).strip().lower()
    if text in {"true", "1", "yes", "on"}:
        return True
    if text in {"false", "0", "no", "off"}:
        return False
    return None


def _parse_sample_observed_at(value: object) -> Optional[datetime]:
    if value in {None, ""}:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def evaluate_byd_vehicle_connection_gate(database: Database, settings: Settings) -> dict[str, Any]:
    latest_samples = database.get_latest_samples()
    byd_sample = next((item for item in latest_samples if item.get("source") == "byd_ev"), None)
    if not byd_sample:
        return {
            "allowed": False,
            "is_connected": False,
            "reason": "No BYD EV sample available",
            "observed_at": None,
        }

    payload = dict(byd_sample.get("raw_payload") or {})
    is_connected = _coerce_optional_bool(payload.get("is_connected"))
    if is_connected is None and _coerce_optional_bool(payload.get("is_charging")) is True:
        is_connected = True

    observed_at = _parse_sample_observed_at(byd_sample.get("observed_at"))
    max_sample_age_seconds = max(300.0, float(settings.byd_poll_seconds) * 2.5)
    age_seconds = None
    if observed_at is not None:
        age_seconds = max(0.0, (datetime.now(timezone.utc) - observed_at).total_seconds())
        if age_seconds > max_sample_age_seconds:
            return {
                "allowed": False,
                "is_connected": False,
                "reason": "BYD EV plug-in state is stale",
                "observed_at": observed_at.isoformat(),
                "age_seconds": round(age_seconds, 1),
                "max_sample_age_seconds": round(max_sample_age_seconds, 1),
                "charging_state": payload.get("charging_state"),
            }

    if is_connected is not True:
        return {
            "allowed": False,
            "is_connected": False,
            "reason": "BYD EV plug-in not detected",
            "observed_at": observed_at.isoformat() if observed_at is not None else None,
            "age_seconds": round(age_seconds, 1) if age_seconds is not None else None,
            "charging_state": payload.get("charging_state"),
        }

    return {
        "allowed": True,
        "is_connected": True,
        "reason": "BYD EV plug-in detected",
        "observed_at": observed_at.isoformat() if observed_at is not None else None,
        "age_seconds": round(age_seconds, 1) if age_seconds is not None else None,
        "charging_state": payload.get("charging_state"),
    }


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
        self._remote_client: Optional[httpx.AsyncClient] = None
        self._failure_started_at: Optional[datetime] = None

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
        usage_rate_w_per_min = pulse_sum / 0.8
        utc_time = datetime.fromtimestamp(timestamp, tz=timezone.utc)
        return {
            "observed_at": utc_time.astimezone(self._melbourne_tz),
            "grid_usage_watts": usage_rate_w_per_min,
            "grid_usage_power_w": usage_rate_w_per_min * 60.0,
            "raw_bytes_hex": data.hex(),
            "pulse_byte_4": int_array[4],
            "pulse_byte_5": int_array[5],
            "pulse_sum": pulse_sum,
            "original_test2_formula": "grid_usage_watts = (byte4 + byte5) / 0.8",
        }

    async def run(self) -> None:
        await self._update_status("ble", state="starting", details={"mac": self.settings.ble_mac})
        if self.settings.remote_ingest_url:
            self._remote_client = httpx.AsyncClient(timeout=10.0)
        while not self._stopped.is_set():
            try:
                await self._update_status("ble", state="connecting", details={"mac": self.settings.ble_mac})
                await self._run_session()
                if not self._stopped.is_set():
                    if self._failure_started_at is None:
                        self._failure_started_at = datetime.now(timezone.utc)
                    await self._update_status(
                        "ble",
                        state="disconnected",
                        error="BLE disconnected",
                        details={"mac": self.settings.ble_mac},
                    )
                    await asyncio.sleep(self.settings.ble_retry_delay_seconds)
            except BleakError as exc:
                LOGGER.warning("BLE error: %s", exc)
                if self._failure_started_at is None:
                    self._failure_started_at = datetime.now(timezone.utc)
                await self._record_error_fallback(str(exc))
                await self._update_status(
                    "ble",
                    state="error",
                    error=str(exc),
                    details={"mac": self.settings.ble_mac},
                )
                await asyncio.sleep(self.settings.ble_retry_delay_seconds)
            except Exception as exc:
                LOGGER.exception("Unexpected BLE failure")
                if self._failure_started_at is None:
                    self._failure_started_at = datetime.now(timezone.utc)
                await self._record_error_fallback(str(exc))
                await self._update_status(
                    "ble",
                    state="error",
                    error=str(exc),
                    details={"mac": self.settings.ble_mac},
                )
                await asyncio.sleep(self.settings.ble_retry_delay_seconds)

    async def stop(self) -> None:
        self._stopped.set()
        if self._remote_client is not None:
            await self._remote_client.aclose()

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
            await self._update_status(
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
        zero_fallback = False
        if self._failure_started_at is not None:
            streak_minutes = (datetime.now(timezone.utc) - self._failure_started_at).total_seconds() / 60.0
            zero_fallback = streak_minutes >= self.settings.ble_zero_after_minutes

        if average_grid is None or zero_fallback:
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
                "failure_started_at": self._failure_started_at.isoformat() if self._failure_started_at else None,
                "zero_after_minutes": self.settings.ble_zero_after_minutes,
                "zero_fallback": zero_fallback,
            },
        )

    async def _on_notification(self, data: bytearray) -> None:
        sample = self._parse_notification(data)
        self._failure_started_at = None
        self.database.insert_sample(
            source="ble",
            observed_at=sample["observed_at"],
            grid_usage_watts=sample["grid_usage_watts"],
            solar_generation_watts=None,
            raw_payload=sample,
        )
        await self._forward_sample(
            source="ble",
            observed_at=sample["observed_at"],
            grid_usage_watts=sample["grid_usage_watts"],
            solar_generation_watts=None,
            raw_payload=sample,
        )
        await self._update_status(
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

    async def _update_status(
        self,
        name: str,
        *,
        state: Optional[str] = None,
        error: Optional[str] = None,
        details: Optional[dict[str, Any]] = None,
        mark_success: bool = False,
    ) -> None:
        await self.statuses.update(
            name,
            state=state,
            error=error,
            details=details,
            mark_success=mark_success,
        )
        if self._remote_client is None or not self.settings.remote_ingest_url:
            return
        try:
            await self._remote_client.post(
                f"{self.settings.remote_ingest_url}/api/ingest/status",
                headers={"X-Ingest-Token": self.settings.remote_ingest_token} if self.settings.remote_ingest_token else {},
                json={
                    "name": name,
                    "state": state,
                    "error": error,
                    "details": details,
                    "mark_success": mark_success,
                },
            )
        except Exception:
            LOGGER.debug("Unable to forward BLE status to remote ingest", exc_info=True)

    async def _forward_sample(
        self,
        *,
        source: str,
        observed_at: datetime,
        grid_usage_watts: Optional[float],
        solar_generation_watts: Optional[float],
        raw_payload: Optional[dict[str, Any]],
    ) -> None:
        if self._remote_client is None or not self.settings.remote_ingest_url:
            return
        try:
            await self._remote_client.post(
                f"{self.settings.remote_ingest_url}/api/ingest/sample",
                headers={"X-Ingest-Token": self.settings.remote_ingest_token} if self.settings.remote_ingest_token else {},
                json={
                    "source": source,
                    "observed_at": observed_at.isoformat(),
                    "grid_usage_watts": grid_usage_watts,
                    "solar_generation_watts": solar_generation_watts,
                    "raw_payload": raw_payload,
                },
            )
        except Exception:
            LOGGER.debug("Unable to forward BLE sample to remote ingest", exc_info=True)


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
                            "latest_grid_usage_w_per_min": watts_to_rate_per_minute(payload.get("grid_usage_watts")),
                            "latest_solar_generation_watts": payload.get("solar_generation_watts"),
                            "latest_solar_generation_w_per_min": watts_to_rate_per_minute(payload.get("solar_generation_watts")),
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
            "grid_usage_power_w": grid_usage,
            "solar_generation_power_w": solar_generation,
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


class NetworkBlePoller:
    STALE_ERROR_CODE = "NETWORK_BLE_STALE"
    FETCH_ERROR_CODE = "NETWORK_BLE_FETCH_ERROR"

    def __init__(self, settings: Settings, database: Database, statuses: StatusRegistry) -> None:
        self.settings = settings
        self.database = database
        self.statuses = statuses
        self._stopped = asyncio.Event()
        self._failure_started_at: Optional[datetime] = None

    async def run(self) -> None:
        await self.statuses.update(
            "network_ble",
            state="starting",
            details={"url": self.settings.network_ble_url},
        )
        async with httpx.AsyncClient(timeout=self.settings.network_ble_timeout_seconds) as client:
            while not self._stopped.is_set():
                try:
                    if not self.settings.network_ble_url:
                        raise RuntimeError("NETWORK_BLE_URL is required when NETWORK_BLE_ENABLED=true")

                    response = await client.get(self.settings.network_ble_url)
                    response.raise_for_status()

                    payload = self._parse_response(response.text)
                    observed_at = datetime.now(timezone.utc)
                    remote_observed_at = self._parse_remote_observed_at(payload.get("remote_observed_at"))

                    if remote_observed_at is not None:
                        stale_minutes = (observed_at - remote_observed_at).total_seconds() / 60.0
                        if stale_minutes >= self.settings.ble_zero_after_minutes:
                            error_message = (
                                "Network BLE feed stale for {minutes:.1f} minutes".format(minutes=stale_minutes)
                            )
                            await self._record_error_fallback(
                                error_message,
                                error_code=self.STALE_ERROR_CODE,
                                battery_percent=payload.get("battery_percent"),
                                remote_observed_at=payload.get("remote_observed_at"),
                                remote_state=payload.get("remote_state"),
                                stale_minutes=stale_minutes,
                                force_zero=True,
                            )
                            await self.statuses.update(
                                "network_ble",
                                state="error",
                                error=error_message,
                                details={
                                    "url": self.settings.network_ble_url,
                                    "error_code": self.STALE_ERROR_CODE,
                                    "battery_percent": payload.get("battery_percent"),
                                    "remote_observed_at": payload.get("remote_observed_at"),
                                    "remote_state": payload.get("remote_state"),
                                    "stale_minutes": round(stale_minutes, 2),
                                    "zero_after_minutes": self.settings.ble_zero_after_minutes,
                                    "latest_observed_at": observed_at.isoformat(),
                                },
                            )
                            await asyncio.sleep(self.settings.network_ble_poll_seconds)
                            continue

                    self._failure_started_at = None
                    self.database.insert_sample(
                        source="ble",
                        observed_at=observed_at,
                        grid_usage_watts=payload["grid_usage_watts"],
                        solar_generation_watts=None,
                        raw_payload=payload,
                    )
                    await self.statuses.update(
                        "network_ble",
                        state="connected",
                        mark_success=True,
                        details={
                            "url": self.settings.network_ble_url,
                            "latest_grid_usage_watts": payload["grid_usage_watts"],
                            "battery_percent": payload.get("battery_percent"),
                            "remote_observed_at": payload.get("remote_observed_at"),
                            "remote_state": payload.get("remote_state"),
                            "latest_observed_at": observed_at.isoformat(),
                        },
                    )
                except httpx.HTTPError as exc:
                    if self._failure_started_at is None:
                        self._failure_started_at = datetime.now(timezone.utc)
                    await self._record_error_fallback(str(exc), error_code=self.FETCH_ERROR_CODE)
                    await self.statuses.update(
                        "network_ble",
                        state="error",
                        error=str(exc),
                        details={
                            "url": self.settings.network_ble_url,
                            "error_code": self.FETCH_ERROR_CODE,
                            "connection_failure_started_at": self._failure_started_at.isoformat(),
                        },
                    )
                except Exception as exc:
                    LOGGER.exception("Unexpected network BLE failure")
                    if self._failure_started_at is None:
                        self._failure_started_at = datetime.now(timezone.utc)
                    await self._record_error_fallback(str(exc), error_code=self.FETCH_ERROR_CODE)
                    await self.statuses.update(
                        "network_ble",
                        state="error",
                        error=str(exc),
                        details={
                            "url": self.settings.network_ble_url,
                            "error_code": self.FETCH_ERROR_CODE,
                            "connection_failure_started_at": self._failure_started_at.isoformat(),
                        },
                    )

                await asyncio.sleep(self.settings.network_ble_poll_seconds)

    async def stop(self) -> None:
        self._stopped.set()

    async def _record_error_fallback(
        self,
        error_message: str,
        *,
        error_code: str,
        battery_percent: Optional[int] = None,
        remote_observed_at: Optional[str] = None,
        remote_state: Optional[str] = None,
        stale_minutes: Optional[float] = None,
        force_zero: bool = False,
    ) -> None:
        observed_at = datetime.now(timezone.utc)
        use_zero_fallback = False
        if force_zero:
            use_zero_fallback = True
        elif self._failure_started_at is not None:
            streak_minutes = (observed_at - self._failure_started_at).total_seconds() / 60.0
            use_zero_fallback = streak_minutes >= self.settings.ble_zero_after_minutes

        average_grid = self.database.get_recent_average(
            source="ble",
            column="grid_usage_watts",
            count=self.settings.failure_average_window,
        )
        payload = {
            "grid_usage_watts": 0.0 if use_zero_fallback else (average_grid if average_grid is not None else 0.0),
            "url": self.settings.network_ble_url,
            "fallback_reason": error_message,
            "error_code": error_code,
            "imputed": True,
            "average_window": self.settings.failure_average_window,
            "zero_after_minutes": self.settings.ble_zero_after_minutes,
            "used_zero_fallback": use_zero_fallback,
            "source_kind": "network_ble",
            "battery_percent": battery_percent,
            "remote_observed_at": remote_observed_at,
            "remote_state": remote_state,
            "stale_minutes": stale_minutes,
        }
        self.database.insert_sample(
            source="ble",
            observed_at=observed_at,
            grid_usage_watts=payload["grid_usage_watts"],
            solar_generation_watts=None,
            raw_payload=payload,
        )

    @staticmethod
    def _parse_remote_observed_at(observed_at_text: Optional[str]) -> Optional[datetime]:
        if not observed_at_text:
            return None
        try:
            parsed = datetime.fromisoformat(str(observed_at_text).replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc)
        except ValueError:
            LOGGER.debug("Unable to parse network BLE remote_observed_at %r", observed_at_text)
            return None

    def _parse_response(self, body_text: str) -> dict[str, Any]:
        lines = [line.strip() for line in body_text.splitlines()]

        def read_line(index: int) -> Optional[str]:
            if 0 <= index < len(lines):
                value = lines[index].strip()
                return value or None
            return None

        usage_text = read_line(self.settings.network_ble_usage_line_index)
        if usage_text is None:
            raise ValueError("Network BLE page did not include a usage line")

        battery_text = read_line(self.settings.network_ble_battery_line_index)
        remote_observed_at = read_line(self.settings.network_ble_timestamp_line_index)
        remote_state = read_line(self.settings.network_ble_state_line_index)

        usage_rate_w_per_min = float(usage_text)
        return {
            "grid_usage_watts": usage_rate_w_per_min,
            "grid_usage_power_w": usage_rate_w_per_min * 60.0,
            "battery_percent": int(float(battery_text)) if battery_text not in (None, "") else None,
            "remote_observed_at": remote_observed_at,
            "remote_state": remote_state,
            "url": self.settings.network_ble_url,
            "source_kind": "network_ble",
        }


class TuyaCloudClient:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._access_token: Optional[str] = None
        self._token_expires_at = 0.0

    def _build_headers(self, method: str, path: str, access_token: str = "", body: bytes = b"") -> dict[str, str]:
        timestamp = str(int(datetime.now(timezone.utc).timestamp() * 1000))
        nonce = uuid.uuid4().hex
        content_sha256 = hashlib.sha256(body).hexdigest()
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

    async def send_device_commands(self, client: httpx.AsyncClient, commands: list[dict[str, Any]]) -> dict[str, Any]:
        token = await self.get_access_token(client)
        path = "/v1.0/devices/{device_id}/commands".format(device_id=self.settings.tuya_device_id)
        body = json.dumps({"commands": commands}, separators=(",", ":")).encode()
        response = await client.post(
            "{base}{path}".format(base=self.settings.tuya_base_url, path=path),
            headers={
                **self._build_headers("POST", path, token, body),
                "Content-Type": "application/json",
            },
            content=body,
        )
        response.raise_for_status()
        payload = response.json()
        if not payload.get("success"):
            raise RuntimeError("Tuya command request failed: {code} {msg}".format(
                code=payload.get("code"),
                msg=payload.get("msg"),
            ))
        return payload


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


class TuyaSolarChargingAutomation:
    def __init__(self, settings: Settings, database: Database, statuses: StatusRegistry) -> None:
        self.settings = settings
        self.database = database
        self.statuses = statuses
        self._stopped = asyncio.Event()
        self._client = TuyaCloudClient(settings)
        self._timezone = pytz.timezone(settings.timezone_name)
        self._ble_guard_hold_until: Optional[datetime] = None
        self._offpeak_was_active = False

    async def run(self) -> None:
        await self.statuses.update(
            "tuya_automation",
            state="starting",
            details={"mode": "solar_surplus"},
        )
        async with httpx.AsyncClient(timeout=self.settings.tuya_timeout_seconds) as client:
            while not self._stopped.is_set():
                try:
                    if self.settings.tuya_manual_override_enabled:
                        evaluation = {
                            "mode": "target",
                            "reason": "Manual override active",
                            "target_enabled": True,
                            "target_current": self._manual_override_current(),
                            "manual_override_active": True,
                        }
                        action = await self._apply_target(client, evaluation)
                        await self.statuses.update(
                            "tuya_automation",
                            state="waiting" if action.get("action") == "blocked_byd_unplugged" else "connected",
                            mark_success=True,
                            details={**evaluation, **action},
                        )
                        await asyncio.sleep(self.settings.tuya_solar_automation_poll_seconds)
                        continue

                    if not self.settings.tuya_solar_automation_enabled:
                        self._ble_guard_hold_until = None
                        await self.statuses.update(
                            "tuya_automation",
                            state="idle",
                            mark_success=True,
                            details={
                                "mode": "disabled",
                                "reason": "Automation toggle is off",
                                "target_enabled": None,
                                "target_current": None,
                            },
                        )
                        await asyncio.sleep(self.settings.tuya_solar_automation_poll_seconds)
                        continue

                    if not self.settings.tuya_access_id or not self.settings.tuya_access_secret or not self.settings.tuya_device_id:
                        raise RuntimeError("Tuya solar automation requires TUYA_ACCESS_ID, TUYA_ACCESS_SECRET, and TUYA_DEVICE_ID")

                    evaluation = self._evaluate_target()
                    if evaluation["mode"] == "offpeak":
                        await self.statuses.update(
                            "tuya_automation",
                            state="idle",
                            mark_success=True,
                            details=evaluation,
                        )
                    elif evaluation["mode"] == "waiting":
                        await self.statuses.update(
                            "tuya_automation",
                            state="waiting",
                            mark_success=True,
                            details=evaluation,
                        )
                    else:
                        action = await self._apply_target(client, evaluation)
                        await self.statuses.update(
                            "tuya_automation",
                            state="waiting" if action.get("action") == "blocked_byd_unplugged" else "connected",
                            mark_success=True,
                            details={**evaluation, **action},
                        )
                except Exception as exc:
                    LOGGER.warning("Tuya solar automation error: %s", exc)
                    await self.statuses.update(
                        "tuya_automation",
                        state="error",
                        error=str(exc),
                        details={"mode": "solar_surplus"},
                    )
                await asyncio.sleep(self.settings.tuya_solar_automation_poll_seconds)

    async def stop(self) -> None:
        self._stopped.set()

    def _evaluate_target(self) -> dict[str, Any]:
        now_utc = datetime.now(timezone.utc)
        now_local = now_utc.astimezone(self._timezone)
        if self._is_offpeak_charge_window(now_local):
            self._offpeak_was_active = True
            self._ble_guard_hold_until = None
            return {
                "mode": "target",
                "reason": "Off-peak schedule active",
                "target_enabled": True,
                "target_current": 13,
                "offpeak_active": True,
                "local_time": now_local.isoformat(),
            }
        if self._offpeak_was_active:
            self._offpeak_was_active = False
            self._ble_guard_hold_until = None
            return {
                "mode": "target",
                "reason": "Off-peak schedule ended",
                "target_enabled": False,
                "target_current": None,
                "offpeak_ended": True,
                "local_time": now_local.isoformat(),
            }
        ble_guard = self._evaluate_ble_guard(now_utc)
        if ble_guard is not None:
            return ble_guard

        window_minutes = max(1.0, float(self.settings.tuya_solar_automation_window_minutes))
        since = now_utc - timedelta(minutes=window_minutes)
        local_site_samples = [
            item for item in self.database.get_samples_range(
                since=since,
                until=now_utc,
                limit=5000,
            )
            if item.get("source") == "local_site" and item.get("solar_generation_watts") is not None
        ]
        if len(local_site_samples) < 2:
            return {
                "mode": "waiting",
                "reason": "Not enough local site solar samples yet",
                "sample_count": len(local_site_samples),
                "window_minutes": window_minutes,
            }

        first_observed_at = self._parse_observed_at(str(local_site_samples[0]["observed_at"]))
        last_observed_at = self._parse_observed_at(str(local_site_samples[-1]["observed_at"]))
        span_seconds = max(0.0, (last_observed_at - first_observed_at).total_seconds())
        allowed_gap_seconds = max(30.0, self.settings.local_site_poll_seconds * 2.0)
        minimum_span_seconds = max(60.0, window_minutes * 60.0 - allowed_gap_seconds)
        if span_seconds < minimum_span_seconds:
            return {
                "mode": "waiting",
                "reason": "Waiting for a full sustained solar window",
                "sample_count": len(local_site_samples),
                "window_minutes": window_minutes,
                "window_span_seconds": round(span_seconds, 1),
                "required_span_seconds": round(minimum_span_seconds, 1),
            }

        solar_values = [
            float(item["solar_generation_watts"])
            for item in local_site_samples
            if item.get("solar_generation_watts") is not None
        ]
        if not solar_values:
            return {
                "mode": "waiting",
                "reason": "No usable solar values in recent window",
                "sample_count": len(local_site_samples),
                "window_minutes": window_minutes,
            }

        min_solar = min(solar_values)
        max_solar = max(solar_values)
        average_solar = sum(solar_values) / len(solar_values)
        min_solar_kw = self._rate_per_minute_to_kw_per_hour(min_solar)
        max_solar_kw = self._rate_per_minute_to_kw_per_hour(max_solar)
        average_solar_kw = self._rate_per_minute_to_kw_per_hour(average_solar)
        target_current = None
        target_enabled = None
        reason = "No average threshold change"
        if average_solar_kw >= float(self.settings.tuya_solar_automation_13a_watts):
            target_enabled = True
            target_current = 13
            reason = "Average solar surplus for 13A"
        elif average_solar_kw >= float(self.settings.tuya_solar_automation_10a_watts):
            target_enabled = True
            target_current = 10
            reason = "Average solar surplus for 10A"
        elif average_solar_kw >= float(self.settings.tuya_solar_automation_6a_watts):
            target_enabled = True
            target_current = 6
            reason = "Average solar surplus for 6A"
        elif average_solar_kw < float(self.settings.tuya_solar_automation_6a_watts):
            target_enabled = False
            reason = "Average solar below charging threshold"

        mode = "target" if target_enabled is not None else "waiting"
        return {
            "mode": mode,
            "reason": reason,
            "window_minutes": window_minutes,
            "sample_count": len(local_site_samples),
            "window_span_seconds": round(span_seconds, 1),
            "min_solar_watts": round(min_solar, 1),
            "max_solar_watts": round(max_solar, 1),
            "average_solar_watts": round(average_solar, 1),
            "min_solar_kw": round(min_solar_kw, 3),
            "max_solar_kw": round(max_solar_kw, 3),
            "average_solar_kw": round(average_solar_kw, 3),
            "target_enabled": target_enabled,
            "target_current": target_current,
            "latest_observed_at": str(local_site_samples[-1]["observed_at"]),
        }

    def _evaluate_ble_guard(self, now_utc: datetime) -> dict[str, Any] | None:
        if self._ble_guard_hold_until is not None and now_utc < self._ble_guard_hold_until:
            return {
                "mode": "target",
                "reason": "BLE grid guard cooldown active",
                "target_enabled": False,
                "target_current": None,
                "ble_guard_active": True,
                "ble_guard_hold_until": self._ble_guard_hold_until.isoformat(),
                "ble_guard_remaining_minutes": round((self._ble_guard_hold_until - now_utc).total_seconds() / 60.0, 1),
            }

        window_minutes = max(1.0, float(self.settings.tuya_ble_guard_window_minutes))
        since = now_utc - timedelta(minutes=window_minutes)
        ble_samples = [
            item for item in self.database.get_samples_range(
                since=since,
                until=now_utc,
                limit=5000,
            )
            if item.get("source") == "ble" and item.get("grid_usage_watts") is not None
        ]
        if len(ble_samples) < 2:
            return None

        first_observed_at = self._parse_observed_at(str(ble_samples[0]["observed_at"]))
        last_observed_at = self._parse_observed_at(str(ble_samples[-1]["observed_at"]))
        span_seconds = max(0.0, (last_observed_at - first_observed_at).total_seconds())
        allowed_gap_seconds = max(30.0, self.settings.ble_retry_delay_seconds * 2.0)
        minimum_span_seconds = max(60.0, window_minutes * 60.0 - allowed_gap_seconds)
        if span_seconds < minimum_span_seconds:
            return None

        ble_values = [
            float(item["grid_usage_watts"])
            for item in ble_samples
            if item.get("grid_usage_watts") is not None
        ]
        if not ble_values:
            return None

        min_ble = min(ble_values)
        max_ble = max(ble_values)
        average_ble = sum(ble_values) / len(ble_values)
        min_ble_kw = self._rate_per_minute_to_kw_per_hour(min_ble)
        max_ble_kw = self._rate_per_minute_to_kw_per_hour(max_ble)
        average_ble_kw = self._rate_per_minute_to_kw_per_hour(average_ble)
        guard_watts = float(self.settings.tuya_ble_guard_watts)
        if average_ble_kw < guard_watts:
            return None

        cooldown_minutes = max(1.0, float(self.settings.tuya_ble_guard_cooldown_minutes))
        self._ble_guard_hold_until = now_utc + timedelta(minutes=cooldown_minutes)
        return {
            "mode": "target",
            "reason": "BLE grid guard triggered by average import",
            "target_enabled": False,
            "target_current": None,
            "ble_guard_active": True,
            "ble_guard_triggered": True,
            "ble_guard_watts": guard_watts,
            "ble_guard_window_minutes": window_minutes,
            "ble_guard_cooldown_minutes": cooldown_minutes,
            "ble_guard_hold_until": self._ble_guard_hold_until.isoformat(),
            "ble_sample_count": len(ble_samples),
            "ble_window_span_seconds": round(span_seconds, 1),
            "min_ble_watts": round(min_ble, 1),
            "max_ble_watts": round(max_ble, 1),
            "average_ble_watts": round(average_ble, 1),
            "min_ble_kw": round(min_ble_kw, 3),
            "max_ble_kw": round(max_ble_kw, 3),
            "average_ble_kw": round(average_ble_kw, 3),
            "latest_observed_at": str(ble_samples[-1]["observed_at"]),
        }

    async def _apply_target(self, client: httpx.AsyncClient, evaluation: dict[str, Any]) -> dict[str, Any]:
        desired_enabled = evaluation.get("target_enabled")
        desired_current = evaluation.get("target_current")
        byd_gate = evaluate_byd_vehicle_connection_gate(self.database, self.settings)
        if not byd_gate.get("allowed"):
            return {
                "action": "blocked_byd_unplugged",
                "device_status": None,
                "byd_vehicle_gate": byd_gate,
                "blocked_reason": byd_gate.get("reason"),
            }
        async with tuya_command_lock:
            initial_status = self._status_map(await self._client.get_device_status(client))
            is_on = self._is_on(initial_status)
            current_value = self._read_current(initial_status)

            if desired_enabled is False:
                if is_on is True:
                    final_status = await self._set_switch_state(client, enabled=False)
                    return {
                        "action": "switched_off",
                        "device_status": final_status,
                    }
                return {
                    "action": "unchanged_off",
                    "device_status": initial_status,
                }

            if desired_enabled is not True or desired_current not in {6, 10, 13}:
                return {
                    "action": "no_change",
                    "device_status": initial_status,
                }

            if is_on is True and current_value == desired_current:
                return {
                    "action": "unchanged_on",
                    "device_status": initial_status,
                }

            if is_on is True:
                await self._set_switch_state(client, enabled=False)
                await asyncio.sleep(1.0)

            if current_value != desired_current:
                await self._client.send_device_commands(
                    client,
                    [{"code": "charge_cur_set", "value": desired_current}],
                )
                await asyncio.sleep(1.0)

            final_status = await self._set_switch_state(client, enabled=True)
            return {
                "action": "set_current_and_on" if current_value != desired_current else "switched_on",
                "device_status": final_status,
            }

    async def _set_switch_state(self, client: httpx.AsyncClient, *, enabled: bool) -> dict[str, Any]:
        await self._client.send_device_commands(
            client,
            [{"code": "switch", "value": enabled}],
        )
        return await self._wait_for_state(client, desired_on=enabled, timeout_seconds=10.0)

    async def _wait_for_state(
        self,
        client: httpx.AsyncClient,
        *,
        desired_on: bool,
        timeout_seconds: float = 10.0,
        poll_interval_seconds: float = 1.0,
    ) -> dict[str, Any]:
        deadline = asyncio.get_running_loop().time() + timeout_seconds
        last_status: dict[str, Any] = {}
        while True:
            last_status = self._status_map(await self._client.get_device_status(client))
            if self._is_on(last_status) is desired_on:
                return last_status
            if asyncio.get_running_loop().time() >= deadline:
                raise RuntimeError(
                    f"Timed out waiting for charger to reach {'ON' if desired_on else 'OFF'} state"
                )
            await asyncio.sleep(poll_interval_seconds)

    @staticmethod
    def _status_map(status_payload: list[dict[str, Any]]) -> dict[str, Any]:
        return {
            str(item.get("code")): item.get("value")
            for item in status_payload
            if isinstance(item, dict) and item.get("code") is not None
        }

    @staticmethod
    def _is_on(status_map: dict[str, Any]) -> Optional[bool]:
        work_state = str(status_map.get("work_state") or "")
        if work_state in {"charger_charging", "charger_wait"}:
            return True
        if work_state in {"charge_end", "charger_free"}:
            return False
        switch_value = status_map.get("switch")
        return switch_value if isinstance(switch_value, bool) else None

    @staticmethod
    def _read_current(status_map: dict[str, Any]) -> Optional[int]:
        value = status_map.get("charge_cur_set")
        try:
            numeric = int(float(value))
        except (TypeError, ValueError):
            return None
        return numeric if numeric in {6, 10, 13} else numeric

    @staticmethod
    def _parse_observed_at(value: str) -> datetime:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed

    def _is_offpeak_charge_window(self, now_local: datetime) -> bool:
        if not self.settings.tuya_offpeak_charge_enabled:
            return False
        start_hour = max(0, min(23, int(self.settings.tuya_offpeak_start_hour)))
        end_hour = max(0, min(23, int(self.settings.tuya_offpeak_end_hour)))
        current_hour = now_local.hour
        if start_hour == end_hour:
            return True
        if start_hour < end_hour:
            return start_hour <= current_hour < end_hour
        return current_hour >= start_hour or current_hour < end_hour

    @staticmethod
    def _rate_per_minute_to_kw_per_hour(value: float) -> float:
        return (float(value) * 60.0) / 1000.0

    def _manual_override_current(self) -> int:
        current = int(getattr(self.settings, "tuya_manual_override_current", 6) or 6)
        return current if current in {6, 10, 13} else 6



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
                observed_at = self._resolve_sample_observed_at(sample)
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

    @staticmethod
    def _resolve_sample_observed_at(sample: dict[str, Any]) -> datetime:
        observed_at_text = sample.get("observed_at") or sample.get("realtime_timestamp_utc") or sample.get("charging_update_time_utc")
        if observed_at_text:
            try:
                parsed = datetime.fromisoformat(str(observed_at_text).replace("Z", "+00:00"))
                if parsed.tzinfo is None:
                    parsed = parsed.replace(tzinfo=timezone.utc)
                return parsed.astimezone(timezone.utc)
            except ValueError:
                LOGGER.debug("Unable to parse BYD observed_at %r", observed_at_text)
        return datetime.now(timezone.utc)

    async def _fetch_sample(self) -> dict[str, Any]:
        env = os.environ.copy()
        process = await asyncio.create_subprocess_exec(
            sys.executable,
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

        power_w = payload.get("gl_w")
        if power_w in (None, ""):
            power_w = payload.get("power_w")
        if power_w in (None, ""):
            power_w = payload.get("total_power_w")
        raw_power_w = float(power_w) if power_w is not None else None
        vehicle_speed_kph = payload.get("vehicle_speed_kph")
        numeric_speed_kph = float(vehicle_speed_kph) if vehicle_speed_kph not in (None, "") else None
        tracked_power_w = max(0.0, raw_power_w) if raw_power_w is not None else 0.0
        if numeric_speed_kph is not None and numeric_speed_kph > 0:
            tracked_power_w = 0.0
        charging_rate_w_per_min = tracked_power_w / 60.0
        payload["power_w"] = raw_power_w
        payload["vehicle_speed_kph"] = numeric_speed_kph
        payload["tracked_power_w"] = tracked_power_w
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

        if self.settings.network_ble_enabled:
            network_ble_poller = NetworkBlePoller(self.settings, self.database, self.statuses)
            self.pollers.append(network_ble_poller)
            self.tasks.append(asyncio.create_task(network_ble_poller.run(), name="network-ble-poller"))

        if self.settings.local_site_enabled:
            site_poller = LocalSitePoller(self.settings, self.database, self.statuses)
            self.pollers.append(site_poller)
            self.tasks.append(asyncio.create_task(site_poller.run(), name="local-site-poller"))

        if self.settings.byd_enabled:
            byd_poller = BydEvPoller(self.settings, self.database, self.statuses)
            self.pollers.append(byd_poller)
            self.tasks.append(asyncio.create_task(byd_poller.run(), name="byd-ev-poller"))

        if self.settings.tuya_access_id and self.settings.tuya_access_secret and self.settings.tuya_device_id:
            tuya_automation = TuyaSolarChargingAutomation(self.settings, self.database, self.statuses)
            self.pollers.append(tuya_automation)
            self.tasks.append(asyncio.create_task(tuya_automation.run(), name="tuya-solar-automation"))

    async def stop(self) -> None:
        for poller in self.pollers:
            await poller.stop()
        for task in self.tasks:
            task.cancel()
        if self.tasks:
            await asyncio.gather(*self.tasks, return_exceptions=True)

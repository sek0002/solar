from __future__ import annotations

import asyncio
import json
import logging
import re
import struct
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional

import httpx
import pytz
from bleak import BleakClient, BleakError

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

    def _parse_notification(self, data: bytearray) -> dict[str, Any]:
        if len(data) < 6:
            raise ValueError(f"Expected at least 6 BLE bytes, received {len(data)}")
        timestamp = struct.unpack_from("<I", data, 0)[0]
        total_pulses = struct.unpack_from("<H", data, 4)[0]
        batch_minutes = max(1, self.settings.ble_reading_batch_size_minutes)
        pulses_per_kwh = max(self.settings.ble_pulses_per_kwh, 1.0)
        usage_watts = total_pulses * (1000.0 / pulses_per_kwh) / batch_minutes
        calculated_kw = total_pulses * ((60.0 / batch_minutes) / pulses_per_kwh)
        utc_time = datetime.fromtimestamp(timestamp, tz=timezone.utc)
        return {
            "observed_at": utc_time.astimezone(self._melbourne_tz),
            "grid_usage_watts": usage_watts,
            "raw_bytes_hex": data.hex(),
            "total_pulses": total_pulses,
            "reading_batch_size_minutes": batch_minutes,
            "pulses_per_kwh": pulses_per_kwh,
            "calculated_kw": calculated_kw,
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

        def notification_handler(_: Any, data: bytearray) -> None:
            asyncio.create_task(self._on_notification(bytearray(data)))

        client = BleakClient(self.settings.ble_mac)
        await client.connect(timeout=self.settings.ble_connection_timeout_seconds)
        try:
            await self.statuses.update(
                "ble",
                state="connecting",
                details={"mac": self.settings.ble_mac},
            )

            battery_level = None
            try:
                battery_value = await client.read_gatt_char(BATTERY_CHAR)
                if battery_value:
                    battery_level = int(battery_value[0])
            except Exception as exc:
                LOGGER.debug("Unable to read Powerpal battery level", exc_info=exc)

            await client.write_gatt_char(
                PAIRING_CODE_CHAR,
                self.convert_pairing_code(self.settings.ble_pairing_code),
                response=True,
            )
            await client.write_gatt_char(
                POWERPAL_FREQ_CHAR,
                batch_size_bytes,
                response=True,
            )

            current_batch_minutes = self.settings.ble_reading_batch_size_minutes
            try:
                await client.read_gatt_char(NOTIFY_CHAR)
            except Exception as exc:
                LOGGER.debug("Initial notify characteristic read failed", exc_info=exc)

            await client.start_notify(NOTIFY_CHAR, notification_handler)
            await self.statuses.update(
                "ble",
                state="connected",
                details={
                    "mac": self.settings.ble_mac,
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
        finally:
            await client.disconnect()

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
                        await self._record_error_fallback(
                            "HTTP 404 from {url}".format(url=self.settings.local_site_url),
                            average_window=self.settings.local_site_404_average_window,
                        )
                        await self.statuses.update(
                            "local_site",
                            state="error",
                            error=f"HTTP 404 from {self.settings.local_site_url}",
                            details={"url": self.settings.local_site_url, "status_code": 404},
                        )
                        await asyncio.sleep(self.settings.local_site_poll_seconds)
                        continue

                    response.raise_for_status()
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
                    await self._record_error_fallback(str(exc))
                    await self.statuses.update(
                        "local_site",
                        state="error",
                        error=str(exc),
                        details={"url": self.settings.local_site_url},
                    )
                except Exception as exc:
                    LOGGER.exception("Unexpected local site failure")
                    await self._record_error_fallback(str(exc))
                    await self.statuses.update(
                        "local_site",
                        state="error",
                        error=str(exc),
                        details={"url": self.settings.local_site_url},
                    )

                await asyncio.sleep(self.settings.local_site_poll_seconds)

    async def stop(self) -> None:
        self._stopped.set()

    async def _record_error_fallback(self, error_message: str, average_window: Optional[int] = None) -> None:
        if not self.settings.local_site_zero_on_error:
            return

        window = average_window or self.settings.failure_average_window
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

        observed_at = datetime.now(timezone.utc)
        payload = {
            "content_type": None,
            "grid_usage_watts": average_grid if average_grid is not None else 0.0,
            "solar_generation_watts": average_solar if average_solar is not None else 0.0,
            "url": self.settings.local_site_url,
            "fallback_reason": error_message,
            "imputed": True,
            "average_window": window,
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

    async def stop(self) -> None:
        for poller in self.pollers:
            await poller.stop()
        for task in self.tasks:
            task.cancel()
        if self.tasks:
            await asyncio.gather(*self.tasks, return_exceptions=True)

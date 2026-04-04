from __future__ import annotations

import asyncio
import json
import logging
import re
import struct
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import httpx
import pytz
from bleak import BleakClient
from bleak.exc import BleakError

from app.config import Settings
from app.database import Database


LOGGER = logging.getLogger(__name__)

PAIRING_CODE_CHAR = "59da0011-12f4-25a6-7d4f-55961dce4205"
POWERPAL_FREQ_CHAR = "59da0013-12f4-25a6-7d4f-55961dce4205"
NOTIFY_CHAR = "59da0001-12f4-25a6-7d4f-55961dce4205"


@dataclass
class PollerStatus:
    name: str
    state: str = "idle"
    last_success_at: str | None = None
    last_error_at: str | None = None
    last_error: str | None = None
    details: dict[str, Any] = field(default_factory=dict)


class StatusRegistry:
    def __init__(self) -> None:
        self._statuses: dict[str, PollerStatus] = {}
        self._lock = asyncio.Lock()

    async def update(
        self,
        name: str,
        *,
        state: str | None = None,
        error: str | None = None,
        details: dict[str, Any] | None = None,
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
        self._disconnect_event = asyncio.Event()
        self._melbourne_tz = pytz.timezone(settings.timezone_name)

    @staticmethod
    def convert_pairing_code(original_pairing_code: str) -> bytes:
        return int(original_pairing_code).to_bytes(4, byteorder="little")

    def _parse_notification(self, data: bytearray) -> dict[str, Any]:
        if len(data) < 6:
            raise ValueError(f"Expected at least 6 BLE bytes, received {len(data)}")
        int_array = list(data)
        timestamp = struct.unpack_from("<I", data, 0)[0]
        pulse_sum = int_array[4] + int_array[5]
        usage_watts = pulse_sum / 0.8
        utc_time = datetime.fromtimestamp(timestamp, tz=timezone.utc)
        return {
            "observed_at": utc_time.astimezone(self._melbourne_tz),
            "grid_usage_watts": usage_watts,
            "raw_bytes_hex": data.hex(),
            "pulse_sum": pulse_sum,
        }

    async def run(self) -> None:
        await self.statuses.update("ble", state="starting", details={"mac": self.settings.ble_mac})
        while not self._stopped.is_set():
            client: BleakClient | None = None
            try:
                self._disconnect_event.clear()
                await self.statuses.update("ble", state="connecting", details={"mac": self.settings.ble_mac})
                client = BleakClient(
                    self.settings.ble_mac,
                    disconnected_callback=self._on_disconnect,
                )
                await client.connect(timeout=self.settings.ble_connection_timeout_seconds)
                await client.write_gatt_char(
                    PAIRING_CODE_CHAR,
                    self.convert_pairing_code(self.settings.ble_pairing_code),
                    response=True,
                )
                await client.write_gatt_char(POWERPAL_FREQ_CHAR, b"\x01\x00\x00\x00", response=True)
                await client.start_notify(NOTIFY_CHAR, self._handle_notification)
                await self.statuses.update("ble", state="connected", mark_success=True)
                done, pending = await asyncio.wait(
                    [
                        asyncio.create_task(self._stopped.wait()),
                        asyncio.create_task(self._disconnect_event.wait()),
                    ],
                    return_when=asyncio.FIRST_COMPLETED,
                )
                for task in done:
                    task.result()
                for task in pending:
                    task.cancel()
                if self._disconnect_event.is_set() and not self._stopped.is_set():
                    await asyncio.sleep(self.settings.ble_retry_delay_seconds)
            except BleakError as exc:
                LOGGER.warning("BLE error: %s", exc)
                await self.statuses.update(
                    "ble",
                    state="error",
                    error=str(exc),
                    details={"mac": self.settings.ble_mac},
                )
                await asyncio.sleep(self.settings.ble_retry_delay_seconds)
            except Exception as exc:
                LOGGER.exception("Unexpected BLE failure")
                await self.statuses.update(
                    "ble",
                    state="error",
                    error=str(exc),
                    details={"mac": self.settings.ble_mac},
                )
                await asyncio.sleep(self.settings.ble_retry_delay_seconds)
            finally:
                if client and client.is_connected:
                    try:
                        await client.stop_notify(NOTIFY_CHAR)
                    except Exception:
                        LOGGER.debug("Unable to stop notifications cleanly", exc_info=True)
                    await client.disconnect()

    async def stop(self) -> None:
        self._stopped.set()

    def _on_disconnect(self, _: BleakClient) -> None:
        asyncio.create_task(
            self.statuses.update(
                "ble",
                state="disconnected",
                error="BLE disconnected",
                details={"mac": self.settings.ble_mac},
            )
        )
        self._disconnect_event.set()

    def _handle_notification(self, sender: int, data: bytearray) -> None:
        asyncio.create_task(self._on_notification(sender, data))

    async def _on_notification(self, _: int, data: bytearray) -> None:
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
                "latest_grid_usage_watts": sample["grid_usage_watts"],
                "latest_observed_at": sample["observed_at"].isoformat(),
            },
        )


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
                    await self.statuses.update(
                        "local_site",
                        state="error",
                        error=str(exc),
                        details={"url": self.settings.local_site_url},
                    )
                except Exception as exc:
                    LOGGER.exception("Unexpected local site failure")
                    await self.statuses.update(
                        "local_site",
                        state="error",
                        error=str(exc),
                        details={"url": self.settings.local_site_url},
                    )

                await asyncio.sleep(self.settings.local_site_poll_seconds)

    async def stop(self) -> None:
        self._stopped.set()

    def _parse_response(self, response: httpx.Response) -> dict[str, Any]:
        body_text = response.text
        parsed_json: Any | None = None
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
        )
        solar_generation = self._extract_value(
            parsed_json=parsed_json,
            body_text=body_text,
            json_path=self.settings.local_solar_json_path,
            regex=self.settings.local_solar_regex,
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
        parsed_json: Any | None,
        body_text: str,
        json_path: str,
        regex: str,
    ) -> float | None:
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
                return float(current)

        if regex:
            match = re.search(regex, body_text, flags=re.IGNORECASE | re.MULTILINE)
            if match:
                group = match.group(1) if match.groups() else match.group(0)
                return float(group)

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

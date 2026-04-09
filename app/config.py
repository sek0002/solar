from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv


load_dotenv()


def _env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    return int(value) if value else default


def _env_float(name: str, default: float) -> float:
    value = os.getenv(name)
    return float(value) if value else default


def _env_optional_int(name: str) -> Optional[int]:
    value = os.getenv(name)
    return int(value) if value else None


def _env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


@dataclass
class Settings:
    app_title: str = os.getenv("APP_TITLE", "Solar Monitor")
    database_path: Path = Path(os.getenv("DATABASE_PATH", "data/solar_monitor.db"))
    timezone_name: str = os.getenv("TIMEZONE", "Australia/Melbourne")
    poller_only: bool = _env_bool("POLLER_ONLY", False)
    ble_site_only: bool = _env_bool("BLE_SITE_ONLY", False)

    ble_enabled: bool = _env_bool("BLE_ENABLED", True)
    ble_mac: str = os.getenv("BLE_MAC", "C9:91:09:7A:2C:B9")
    ble_pairing_code: str = os.getenv("BLE_PAIRING_CODE", "774034")
    ble_reading_batch_size_minutes: int = _env_int("BLE_READING_BATCH_SIZE_MINUTES", 1)
    ble_pulses_per_kwh: float = _env_float("BLE_PULSES_PER_KWH", 1000.0)
    ble_retry_delay_seconds: float = _env_float("BLE_RETRY_DELAY_SECONDS", 5.0)
    ble_connection_timeout_seconds: float = _env_float("BLE_CONNECTION_TIMEOUT_SECONDS", 30.0)
    ble_zero_after_minutes: float = _env_float("BLE_ZERO_AFTER_MINUTES", 10.0)
    remote_ingest_url: str = os.getenv("REMOTE_INGEST_URL", "").rstrip("/")
    remote_ingest_token: str = os.getenv("REMOTE_INGEST_TOKEN", "")
    ingest_token: str = os.getenv("INGEST_TOKEN", "")
    network_ble_enabled: bool = _env_bool("NETWORK_BLE_ENABLED", False)
    network_ble_url: str = os.getenv("NETWORK_BLE_URL", "").rstrip("/")
    network_ble_timeout_seconds: float = _env_float("NETWORK_BLE_TIMEOUT_SECONDS", 10.0)
    network_ble_poll_seconds: float = _env_float("NETWORK_BLE_POLL_SECONDS", 15.0)
    network_ble_usage_line_index: int = _env_int("NETWORK_BLE_USAGE_LINE_INDEX", 0)
    network_ble_battery_line_index: int = _env_int("NETWORK_BLE_BATTERY_LINE_INDEX", 1)
    network_ble_timestamp_line_index: int = _env_int("NETWORK_BLE_TIMESTAMP_LINE_INDEX", 2)
    network_ble_state_line_index: int = _env_int("NETWORK_BLE_STATE_LINE_INDEX", 3)

    local_site_enabled: bool = _env_bool("LOCAL_SITE_ENABLED", True)
    local_site_url: str = os.getenv("LOCAL_SITE_URL", "http://127.0.0.1/")
    local_site_timeout_seconds: float = _env_float("LOCAL_SITE_TIMEOUT_SECONDS", 10.0)
    local_site_poll_seconds: float = _env_float("LOCAL_SITE_POLL_SECONDS", 15.0)
    local_site_format: str = os.getenv("LOCAL_SITE_FORMAT", "auto")
    local_usage_json_path: str = os.getenv("LOCAL_USAGE_JSON_PATH", "")
    local_solar_json_path: str = os.getenv("LOCAL_SOLAR_JSON_PATH", "")
    local_usage_regex: str = os.getenv("LOCAL_USAGE_REGEX", "")
    local_solar_regex: str = os.getenv("LOCAL_SOLAR_REGEX", "")
    local_usage_line_index: Optional[int] = _env_optional_int("LOCAL_USAGE_LINE_INDEX")
    local_solar_line_index: Optional[int] = _env_optional_int("LOCAL_SOLAR_LINE_INDEX")
    local_usage_divisor: float = _env_float("LOCAL_USAGE_DIVISOR", 1.0)
    local_solar_divisor: float = _env_float("LOCAL_SOLAR_DIVISOR", 1.0)
    local_usage_multiplier: float = _env_float("LOCAL_USAGE_MULTIPLIER", 1.0)
    local_solar_multiplier: float = _env_float("LOCAL_SOLAR_MULTIPLIER", 1.0)
    local_site_zero_on_error: bool = _env_bool("LOCAL_SITE_ZERO_ON_ERROR", True)
    local_site_404_average_window: int = _env_int("LOCAL_SITE_404_AVERAGE_WINDOW", 5)
    local_site_404_zero_after_minutes: float = _env_float("LOCAL_SITE_404_ZERO_AFTER_MINUTES", 10.0)
    local_site_connection_zero_after_minutes: float = _env_float("LOCAL_SITE_CONNECTION_ZERO_AFTER_MINUTES", 10.0)
    failure_average_window: int = _env_int("FAILURE_AVERAGE_WINDOW", 3)

    byd_enabled: bool = _env_bool("BYD_ENABLED", False)
    byd_node_bin: str = os.getenv("BYD_NODE_BIN", "node")
    byd_re_dir: str = os.getenv("BYD_RE_DIR", "/opt/byd-re")
    byd_vin: str = os.getenv("BYD_VIN", "")
    byd_poll_seconds: float = _env_float("BYD_POLL_SECONDS", 60.0)
    byd_command_timeout_seconds: float = _env_float("BYD_COMMAND_TIMEOUT_SECONDS", 120.0)

    tuya_enabled: bool = _env_bool("TUYA_ENABLED", False)
    tuya_base_url: str = os.getenv("TUYA_BASE_URL", "https://openapi.tuyaeu.com")
    tuya_access_id: str = os.getenv("TUYA_ACCESS_ID", "")
    tuya_access_secret: str = os.getenv("TUYA_ACCESS_SECRET", "")
    tuya_device_id: str = os.getenv("TUYA_DEVICE_ID", "")
    tuya_poll_seconds: float = _env_float("TUYA_POLL_SECONDS", 30.0)
    tuya_timeout_seconds: float = _env_float("TUYA_TIMEOUT_SECONDS", 15.0)
    tuya_voltage_code: str = os.getenv("TUYA_VOLTAGE_CODE", "107")
    tuya_current_code: str = os.getenv("TUYA_CURRENT_CODE", "108")
    tuya_power_code: str = os.getenv("TUYA_POWER_CODE", "power_total")
    tuya_temperature_code: str = os.getenv("TUYA_TEMPERATURE_CODE", "110")
    tuya_session_energy_code: str = os.getenv("TUYA_SESSION_ENERGY_CODE", "charge_energy_once")
    tuya_voltage_divisor: float = _env_float("TUYA_VOLTAGE_DIVISOR", 10.0)
    tuya_current_divisor: float = _env_float("TUYA_CURRENT_DIVISOR", 10.0)
    tuya_power_divisor: float = _env_float("TUYA_POWER_DIVISOR", 100.0)
    tuya_temperature_divisor: float = _env_float("TUYA_TEMPERATURE_DIVISOR", 10.0)
    tuya_session_energy_divisor: float = _env_float("TUYA_SESSION_ENERGY_DIVISOR", 100.0)

    api_default_hours: int = _env_int("API_DEFAULT_HOURS", 24)
    api_max_points: int = _env_int("API_MAX_POINTS", 5000)
    ble_site_host: str = os.getenv("BLE_SITE_HOST", "0.0.0.0")
    ble_site_port: int = _env_int("BLE_SITE_PORT", 8002)


settings = Settings()

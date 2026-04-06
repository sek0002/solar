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

    ble_enabled: bool = _env_bool("BLE_ENABLED", True)
    ble_mac: str = os.getenv("BLE_MAC", "C9:91:09:7A:2C:B9")
    ble_pairing_code: str = os.getenv("BLE_PAIRING_CODE", "774034")
    ble_reading_batch_size_minutes: int = _env_int("BLE_READING_BATCH_SIZE_MINUTES", 1)
    ble_pulses_per_kwh: float = _env_float("BLE_PULSES_PER_KWH", 1000.0)
    ble_retry_delay_seconds: float = _env_float("BLE_RETRY_DELAY_SECONDS", 5.0)
    ble_connection_timeout_seconds: float = _env_float("BLE_CONNECTION_TIMEOUT_SECONDS", 30.0)

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
    failure_average_window: int = _env_int("FAILURE_AVERAGE_WINDOW", 3)

    api_default_hours: int = _env_int("API_DEFAULT_HOURS", 24)
    api_max_points: int = _env_int("API_MAX_POINTS", 5000)


settings = Settings()

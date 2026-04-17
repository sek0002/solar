from __future__ import annotations

import json
import math
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from threading import Lock
from typing import Any, Optional
from zoneinfo import ZoneInfo


class Database:
    def __init__(self, path: Path, timezone_name: str = "Australia/Melbourne") -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = Lock()
        self._timezone = ZoneInfo(timezone_name)
        self._initialize()

    @contextmanager
    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, check_same_thread=False)
        connection.row_factory = sqlite3.Row
        try:
            yield connection
            connection.commit()
        finally:
            connection.close()

    def _initialize(self) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS samples (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source TEXT NOT NULL,
                    observed_at TEXT NOT NULL,
                    grid_usage_watts REAL,
                    solar_generation_watts REAL,
                    raw_payload TEXT
                )
                """
            )
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_samples_observed_at
                ON samples (observed_at)
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS cumulative_samples (
                    source TEXT NOT NULL,
                    observed_at TEXT NOT NULL,
                    day_key TEXT NOT NULL,
                    cumulative_kwh REAL NOT NULL,
                    PRIMARY KEY (source, observed_at)
                )
                """
            )
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_cumulative_samples_source_observed_at
                ON cumulative_samples (source, observed_at)
                """
            )

    def insert_sample(
        self,
        *,
        source: str,
        observed_at: datetime,
        grid_usage_watts: Optional[float],
        solar_generation_watts: Optional[float],
        raw_payload: Optional[dict[str, Any]] = None,
    ) -> None:
        payload = json.dumps(raw_payload, default=self._json_default) if raw_payload is not None else None
        with self._lock, self._connect() as connection:
            observed_at_iso = observed_at.astimezone(timezone.utc).isoformat()
            connection.execute(
                """
                INSERT INTO samples (
                    source,
                    observed_at,
                    grid_usage_watts,
                    solar_generation_watts,
                    raw_payload
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (
                    source,
                    observed_at_iso,
                    grid_usage_watts,
                    solar_generation_watts,
                    payload,
                ),
            )
            self._refresh_cumulative_cache(
                connection,
                source=source,
                observed_at_iso=observed_at_iso,
                grid_usage_watts=grid_usage_watts,
                solar_generation_watts=solar_generation_watts,
            )

    @staticmethod
    def _json_default(value: Any) -> Any:
        if isinstance(value, datetime):
            return value.isoformat()
        raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")

    def get_recent_samples(self, *, hours: int, limit: int) -> list[dict[str, Any]]:
        since = datetime.now(timezone.utc) - timedelta(hours=hours)
        return self.get_samples_range(since=since, until=datetime.now(timezone.utc), limit=limit)

    def get_samples_range(
        self,
        *,
        since: datetime,
        until: datetime,
        limit: int,
    ) -> list[dict[str, Any]]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT source, observed_at, grid_usage_watts, solar_generation_watts, raw_payload
                FROM (
                    SELECT source, observed_at, grid_usage_watts, solar_generation_watts, raw_payload
                    FROM samples
                    WHERE observed_at >= ?
                      AND observed_at <= ?
                    ORDER BY observed_at DESC
                    LIMIT ?
                ) recent_samples
                ORDER BY observed_at ASC
                """,
                (
                    since.astimezone(timezone.utc).isoformat(),
                    until.astimezone(timezone.utc).isoformat(),
                    limit,
                ),
            ).fetchall()

        items: list[dict[str, Any]] = []
        for row in rows:
            payload = json.loads(row["raw_payload"]) if row["raw_payload"] else None
            items.append(
                {
                    "source": row["source"],
                    "observed_at": row["observed_at"],
                    "grid_usage_watts": row["grid_usage_watts"],
                    "solar_generation_watts": row["solar_generation_watts"],
                    "raw_payload": payload,
                }
            )
        return items

    def get_latest_samples(self) -> list[dict[str, Any]]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT source, observed_at, grid_usage_watts, solar_generation_watts, raw_payload
                FROM samples
                WHERE id IN (
                    SELECT MAX(id)
                    FROM samples
                    GROUP BY source
                )
                ORDER BY observed_at DESC
                """
            ).fetchall()

        return [
            {
                "source": row["source"],
                "observed_at": row["observed_at"],
                "grid_usage_watts": row["grid_usage_watts"],
                "solar_generation_watts": row["solar_generation_watts"],
                "raw_payload": json.loads(row["raw_payload"]) if row["raw_payload"] else None,
            }
            for row in rows
        ]

    def get_recent_average(
        self,
        *,
        source: str,
        column: str,
        count: int,
    ) -> Optional[float]:
        if column not in {"grid_usage_watts", "solar_generation_watts"}:
            raise ValueError(f"Unsupported average column: {column}")

        with self._connect() as connection:
            rows = connection.execute(
                f"""
                SELECT {column} AS value
                FROM samples
                WHERE source = ?
                  AND {column} IS NOT NULL
                ORDER BY observed_at DESC
                LIMIT ?
                """,
                (source, count),
            ).fetchall()

        values = [float(row["value"]) for row in rows if row["value"] is not None]
        if not values:
            return None
        return sum(values) / len(values)

    def get_cumulative_samples(self) -> dict[str, list[dict[str, Any]]]:
        with self._lock, self._connect() as connection:
            for source in ("local_site", "ble", "byd_ev"):
                self._ensure_cumulative_cache(connection, source)

            rows = connection.execute(
                """
                SELECT source, observed_at, cumulative_kwh
                FROM cumulative_samples
                WHERE source IN ('local_site', 'ble', 'byd_ev')
                ORDER BY observed_at ASC
                """
            ).fetchall()

        grouped: dict[str, list[dict[str, Any]]] = {"solar": [], "grid": [], "ev": []}
        source_map = {"local_site": "solar", "ble": "grid", "byd_ev": "ev"}
        for row in rows:
            target = source_map.get(row["source"])
            if not target:
                continue
            grouped[target].append(
                {
                    "observed_at": row["observed_at"],
                    "cumulative_kwh": row["cumulative_kwh"],
                }
            )
        return grouped

    def _ensure_cumulative_cache(self, connection: sqlite3.Connection, source: str) -> None:
        has_samples = connection.execute(
            """
            SELECT 1
            FROM samples
            WHERE source = ?
            LIMIT 1
            """,
            (source,),
        ).fetchone()
        if not has_samples:
            return

        has_cache = connection.execute(
            """
            SELECT 1
            FROM cumulative_samples
            WHERE source = ?
            LIMIT 1
            """,
            (source,),
        ).fetchone()
        if has_cache:
            return

        self._rebuild_cumulative_cache(connection, source)

    def _refresh_cumulative_cache(
        self,
        connection: sqlite3.Connection,
        *,
        source: str,
        observed_at_iso: str,
        grid_usage_watts: Optional[float],
        solar_generation_watts: Optional[float],
    ) -> None:
        if source == "local_site":
            if solar_generation_watts is None:
                return
            self._append_or_rebuild_cumulative_cache(connection, source, observed_at_iso, "solar_generation_watts")
            return

        if source == "ble":
            if grid_usage_watts is None:
                return
            self._append_or_rebuild_cumulative_cache(connection, source, observed_at_iso, "grid_usage_watts")
            return

        if source == "byd_ev":
            self._rebuild_cumulative_cache(connection, source)

    def _append_or_rebuild_cumulative_cache(
        self,
        connection: sqlite3.Connection,
        source: str,
        observed_at_iso: str,
        value_column: str,
    ) -> None:
        latest_cached = connection.execute(
            """
            SELECT observed_at, day_key, cumulative_kwh
            FROM cumulative_samples
            WHERE source = ?
            ORDER BY observed_at DESC
            LIMIT 1
            """,
            (source,),
        ).fetchone()

        if latest_cached is None:
            self._rebuild_cumulative_cache(connection, source)
            return

        if observed_at_iso <= latest_cached["observed_at"]:
            self._rebuild_cumulative_cache(connection, source)
            return

        relevant_samples = connection.execute(
            f"""
            SELECT observed_at, {value_column} AS value
            FROM samples
            WHERE source = ?
              AND {value_column} IS NOT NULL
            ORDER BY observed_at DESC
            LIMIT 2
            """,
            (source,),
        ).fetchall()

        if len(relevant_samples) < 2:
            self._rebuild_cumulative_cache(connection, source)
            return

        current_sample = relevant_samples[0]
        previous_sample = relevant_samples[1]
        if current_sample["observed_at"] != observed_at_iso:
            self._rebuild_cumulative_cache(connection, source)
            return

        current_observed_at = self._parse_api_datetime(current_sample["observed_at"])
        previous_observed_at = self._parse_api_datetime(previous_sample["observed_at"])
        average_rate = (float(previous_sample["value"]) + float(current_sample["value"])) / 2.0
        current_day_key = self._day_key(current_observed_at)
        day_energy = sum(
            segment["energy_kwh"]
            for segment in self._split_energy_across_days(previous_observed_at, current_observed_at, average_rate)
            if segment["day_key"] == current_day_key
        )
        cumulative_kwh = (latest_cached["cumulative_kwh"] if latest_cached["day_key"] == current_day_key else 0.0) + day_energy

        connection.execute(
            """
            INSERT OR REPLACE INTO cumulative_samples (source, observed_at, day_key, cumulative_kwh)
            VALUES (?, ?, ?, ?)
            """,
            (source, observed_at_iso, current_day_key, cumulative_kwh),
        )

    def _rebuild_cumulative_cache(self, connection: sqlite3.Connection, source: str) -> None:
        if source == "local_site":
            rows = connection.execute(
                """
                SELECT observed_at, solar_generation_watts AS value
                FROM samples
                WHERE source = 'local_site'
                  AND solar_generation_watts IS NOT NULL
                ORDER BY observed_at ASC
                """
            ).fetchall()
            cumulative_points = self._build_cumulative_points_from_rows(rows)
        elif source == "ble":
            rows = connection.execute(
                """
                SELECT observed_at, grid_usage_watts AS value
                FROM samples
                WHERE source = 'ble'
                  AND grid_usage_watts IS NOT NULL
                ORDER BY observed_at ASC
                """
            ).fetchall()
            cumulative_points = self._build_cumulative_points_from_rows(rows)
        elif source == "byd_ev":
            rows = connection.execute(
                """
                SELECT observed_at, grid_usage_watts, raw_payload
                FROM samples
                WHERE source = 'byd_ev'
                ORDER BY observed_at ASC
                """
            ).fetchall()
            cumulative_points = self._build_byd_cumulative_points(rows)
        else:
            cumulative_points = []

        connection.execute("DELETE FROM cumulative_samples WHERE source = ?", (source,))
        if cumulative_points:
            connection.executemany(
                """
                INSERT INTO cumulative_samples (source, observed_at, day_key, cumulative_kwh)
                VALUES (?, ?, ?, ?)
                """,
                [(source, point["observed_at"], point["day_key"], point["cumulative_kwh"]) for point in cumulative_points],
            )

    def _build_cumulative_points_from_rows(self, rows: list[sqlite3.Row]) -> list[dict[str, Any]]:
        points: list[dict[str, Any]] = []
        cumulative = 0.0
        current_day_key: Optional[str] = None

        for index, row in enumerate(rows):
            current_observed_at = self._parse_api_datetime(row["observed_at"])
            day_key = self._day_key(current_observed_at)

            if day_key != current_day_key:
                cumulative = 0.0
                current_day_key = day_key

            if index > 0:
                previous_row = rows[index - 1]
                previous_observed_at = self._parse_api_datetime(previous_row["observed_at"])
                average_rate = (float(previous_row["value"]) + float(row["value"])) / 2.0
                cumulative += sum(
                    segment["energy_kwh"]
                    for segment in self._split_energy_across_days(previous_observed_at, current_observed_at, average_rate)
                    if segment["day_key"] == day_key
                )

            points.append(
                {
                    "observed_at": row["observed_at"],
                    "day_key": day_key,
                    "cumulative_kwh": cumulative,
                }
            )

        return points

    def _build_byd_cumulative_points(self, rows: list[sqlite3.Row]) -> list[dict[str, Any]]:
        byd_rows: list[dict[str, Any]] = []
        for row in rows:
            payload = json.loads(row["raw_payload"]) if row["raw_payload"] else {}
            byd_rows.append(
                {
                    "observed_at": row["observed_at"],
                    "observed_dt": self._parse_api_datetime(row["observed_at"]),
                    "grid_usage_watts": row["grid_usage_watts"],
                    "raw_payload": payload if isinstance(payload, dict) else {},
                }
            )

        movement_window_seconds = 2 * 60
        moving_epochs = [
            item["observed_dt"].timestamp()
            for item in byd_rows
            if (self._get_byd_vehicle_speed_kph(item) or 0) > 0
        ]

        series_rows: list[dict[str, Any]] = []
        for item in byd_rows:
            charging_rate = self._get_byd_charging_rate(item)
            power_w = self._get_byd_power_watts(item)
            suppressed = any(abs(item["observed_dt"].timestamp() - epoch) <= movement_window_seconds for epoch in moving_epochs)
            if suppressed:
                charging_rate = 0.0
                power_w = 0.0
            if charging_rate is None and power_w is None:
                continue
            series_rows.append(
                {
                    "observed_at": item["observed_at"],
                    "value": charging_rate if charging_rate is not None else 0.0,
                }
            )

        return self._build_cumulative_points_from_rows(series_rows)

    def _split_energy_across_days(
        self,
        start: datetime,
        end: datetime,
        average_rate_w_per_min: float,
    ) -> list[dict[str, Any]]:
        if end <= start:
            return []

        cursor = start.astimezone(self._timezone)
        end_local = end.astimezone(self._timezone)
        segments: list[dict[str, Any]] = []

        while cursor < end_local:
            next_boundary = datetime(cursor.year, cursor.month, cursor.day, tzinfo=self._timezone) + timedelta(days=1)
            segment_end = next_boundary if next_boundary < end_local else end_local
            delta_minutes = (segment_end - cursor).total_seconds() / 60.0
            if delta_minutes > 0:
                segments.append(
                    {
                        "day_key": cursor.strftime("%Y-%m-%d"),
                        "energy_kwh": (float(average_rate_w_per_min) * delta_minutes) / 1000.0,
                    }
                )
            cursor = segment_end

        return segments

    def _day_key(self, observed_at: datetime) -> str:
        return observed_at.astimezone(self._timezone).strftime("%Y-%m-%d")

    @staticmethod
    def _parse_api_datetime(value: str) -> datetime:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed

    @staticmethod
    def _normalize_number(value: Any) -> Optional[float]:
        if value in (None, ""):
            return None
        candidate = value.strip().replace(",", "") if isinstance(value, str) else value
        try:
            numeric = float(candidate)
        except (TypeError, ValueError):
            return None
        return numeric if math.isfinite(numeric) else None

    @staticmethod
    def _coalesce(*values: Any) -> Any:
        for value in values:
            if value is not None:
                return value
        return None

    def _get_byd_vehicle_speed_kph(self, item: dict[str, Any]) -> Optional[float]:
        payload = item.get("raw_payload") or {}
        realtime = payload.get("realtime") if isinstance(payload.get("realtime"), dict) else {}
        vehicle = payload.get("vehicle") if isinstance(payload.get("vehicle"), dict) else {}
        candidate = self._coalesce(
            payload.get("vehicle_speed_kph"),
            realtime.get("speed"),
            realtime.get("speedKmH"),
            realtime.get("speedKmh"),
            realtime.get("vehicleSpeed"),
            vehicle.get("speed"),
        )
        numeric = self._normalize_number(candidate)
        return numeric if numeric is not None else None

    def _get_byd_power_watts(self, item: dict[str, Any]) -> Optional[float]:
        payload = item.get("raw_payload") or {}
        realtime = payload.get("realtime") if isinstance(payload.get("realtime"), dict) else {}
        vehicle = payload.get("vehicle") if isinstance(payload.get("vehicle"), dict) else {}

        vehicle_speed_kph = self._normalize_number(
            self._coalesce(
                payload.get("vehicle_speed_kph"),
                realtime.get("speed"),
                realtime.get("speedKmH"),
                realtime.get("speedKmh"),
                realtime.get("vehicleSpeed"),
                vehicle.get("speed"),
            )
        )
        if vehicle_speed_kph is not None and vehicle_speed_kph > 0:
            return 0.0

        candidates = [
            payload.get("tracked_power_w"),
            payload.get("gl_w"),
            realtime.get("gl"),
            vehicle.get("gl"),
            payload.get("power_w"),
            payload.get("total_power_w"),
            vehicle.get("totalPower"),
        ]
        for candidate in candidates:
            numeric = self._normalize_number(candidate)
            if numeric is not None:
                return max(0.0, numeric)

        grid_usage_watts = item.get("grid_usage_watts")
        if grid_usage_watts is not None:
            numeric_grid_usage = self._normalize_number(grid_usage_watts)
            if numeric_grid_usage is not None:
                return max(0.0, numeric_grid_usage * 60.0)

        return None

    def _get_byd_charging_rate(self, item: dict[str, Any]) -> Optional[float]:
        payload = item.get("raw_payload") or {}
        vehicle_speed_kph = self._get_byd_vehicle_speed_kph(item)
        if vehicle_speed_kph is not None and vehicle_speed_kph > 0:
            return 0.0

        candidates = [
            payload.get("ev_charging_rate_w_per_min"),
            item.get("grid_usage_watts"),
            (tracked_power / 60.0) if (tracked_power := self._normalize_number(payload.get("tracked_power_w"))) is not None else None,
            (max(0.0, power_w) / 60.0) if (power_w := self._normalize_number(payload.get("power_w"))) is not None else None,
        ]

        for candidate in candidates:
            numeric = self._normalize_number(candidate)
            if numeric is not None:
                return max(0.0, numeric)

        return None

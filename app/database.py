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
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS energy_buckets (
                    source TEXT NOT NULL,
                    granularity TEXT NOT NULL,
                    bucket_key TEXT NOT NULL,
                    energy_kwh REAL NOT NULL,
                    PRIMARY KEY (source, granularity, bucket_key)
                )
                """
            )
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_energy_buckets_granularity_bucket
                ON energy_buckets (granularity, bucket_key)
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
            self._refresh_energy_bucket_cache(
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

    def get_energy_summary(self) -> dict[str, Any]:
        with self._lock, self._connect() as connection:
            for source in ("local_site", "ble", "byd_ev"):
                self._ensure_energy_bucket_cache(connection, source)

            hourly_keys = self._recent_hour_bucket_keys(24)
            daily_keys = self._recent_day_bucket_keys(7)
            weekly_keys = self._recent_week_bucket_keys(30)

            generation = {
                "hourly": self._get_bucket_map(connection, "hour", hourly_keys),
                "daily": self._get_bucket_map(connection, "day", daily_keys),
                "weekly": self._get_bucket_map(connection, "week", weekly_keys),
            }

            monthly_key = self._month_key(datetime.now(timezone.utc))
            totals = {
                "daily": self._get_totals_for_key(connection, "day", self._day_key(datetime.now(timezone.utc))),
                "weekly": self._get_totals_for_key(connection, "week", self._week_key(datetime.now(timezone.utc))),
                "monthly": self._get_totals_for_key(connection, "month", monthly_key),
            }

        return {"generation": generation, "totals": totals}

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

    def _ensure_energy_bucket_cache(self, connection: sqlite3.Connection, source: str) -> None:
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
            FROM energy_buckets
            WHERE source = ?
            LIMIT 1
            """,
            (source,),
        ).fetchone()
        if has_cache:
            if source == "ble":
                has_offpeak_cache = connection.execute(
                    """
                    SELECT 1
                    FROM energy_buckets
                    WHERE source = 'ble_offpeak'
                    LIMIT 1
                    """
                ).fetchone()
                if not has_offpeak_cache:
                    self._rebuild_energy_bucket_cache(connection, source)
            return

        self._rebuild_energy_bucket_cache(connection, source)

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

    def _refresh_energy_bucket_cache(
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
            self._append_or_rebuild_energy_buckets(connection, source, observed_at_iso, "solar_generation_watts")
            return

        if source == "ble":
            if grid_usage_watts is None:
                return
            self._append_or_rebuild_energy_buckets(connection, source, observed_at_iso, "grid_usage_watts")
            return

        if source == "byd_ev":
            self._rebuild_energy_bucket_cache(connection, source)

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

    def _append_or_rebuild_energy_buckets(
        self,
        connection: sqlite3.Connection,
        source: str,
        observed_at_iso: str,
        value_column: str,
    ) -> None:
        latest_bucket = connection.execute(
            """
            SELECT bucket_key
            FROM energy_buckets
            WHERE source = ?
            LIMIT 1
            """,
            (source,),
        ).fetchone()
        if latest_bucket is None:
            self._rebuild_energy_bucket_cache(connection, source)
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
            self._rebuild_energy_bucket_cache(connection, source)
            return

        current_sample = relevant_samples[0]
        previous_sample = relevant_samples[1]
        if current_sample["observed_at"] != observed_at_iso:
            self._rebuild_energy_bucket_cache(connection, source)
            return
        if observed_at_iso <= previous_sample["observed_at"]:
            self._rebuild_energy_bucket_cache(connection, source)
            return

        current_observed_at = self._parse_api_datetime(current_sample["observed_at"])
        previous_observed_at = self._parse_api_datetime(previous_sample["observed_at"])
        average_rate = (float(previous_sample["value"]) + float(current_sample["value"])) / 2.0
        self._add_bucket_segments(connection, source, previous_observed_at, current_observed_at, average_rate)

    def _rebuild_energy_bucket_cache(self, connection: sqlite3.Connection, source: str) -> None:
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
            bucket_totals = self._build_bucket_totals_from_rows(rows)
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
            bucket_totals = self._build_bucket_totals_from_rows(rows, split_offpeak=True)
        elif source == "byd_ev":
            rows = connection.execute(
                """
                SELECT observed_at, grid_usage_watts, raw_payload
                FROM samples
                WHERE source = 'byd_ev'
                ORDER BY observed_at ASC
                """
            ).fetchall()
            bucket_totals = self._build_byd_bucket_totals(rows)
        else:
            bucket_totals = {}

        connection.execute("DELETE FROM energy_buckets WHERE source IN (?, ?)", (source, f"{source}_offpeak"))
        if bucket_totals:
            connection.executemany(
                """
                INSERT INTO energy_buckets (source, granularity, bucket_key, energy_kwh)
                VALUES (?, ?, ?, ?)
                """,
                [
                    (
                        "ble_offpeak" if source == "ble" and granularity.endswith(":offpeak") else source,
                        granularity.split(":", 1)[0],
                        bucket_key,
                        energy_kwh,
                    )
                    for granularity, bucket_map in bucket_totals.items()
                    for bucket_key, energy_kwh in bucket_map.items()
                ],
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

    def _build_bucket_totals_from_rows(
        self,
        rows: list[sqlite3.Row | dict[str, Any]],
        *,
        split_offpeak: bool = False,
    ) -> dict[str, dict[str, float]]:
        bucket_totals: dict[str, dict[str, float]] = {"hour": {}, "day": {}, "week": {}, "month": {}}
        if split_offpeak:
            bucket_totals.update({"hour:offpeak": {}, "day:offpeak": {}, "week:offpeak": {}, "month:offpeak": {}})
        for index in range(1, len(rows)):
            previous_row = rows[index - 1]
            current_row = rows[index]
            previous_observed_at = self._parse_api_datetime(previous_row["observed_at"])
            current_observed_at = self._parse_api_datetime(current_row["observed_at"])
            average_rate = (float(previous_row["value"]) + float(current_row["value"])) / 2.0
            for granularity, segments in self._build_bucket_segments(
                previous_observed_at,
                current_observed_at,
                average_rate,
                split_offpeak=split_offpeak,
            ).items():
                target = bucket_totals[granularity]
                for bucket_key, energy_kwh in segments.items():
                    target[bucket_key] = target.get(bucket_key, 0.0) + energy_kwh
        return bucket_totals

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

    def _build_byd_bucket_totals(self, rows: list[sqlite3.Row]) -> dict[str, dict[str, float]]:
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

        return self._build_bucket_totals_from_rows(series_rows)

    def _add_bucket_segments(
        self,
        connection: sqlite3.Connection,
        source: str,
        start: datetime,
        end: datetime,
        average_rate_w_per_min: float,
    ) -> None:
        for granularity, bucket_map in self._build_bucket_segments(
            start,
            end,
            average_rate_w_per_min,
            split_offpeak=(source == "ble"),
        ).items():
            for bucket_key, energy_kwh in bucket_map.items():
                connection.execute(
                    """
                    INSERT INTO energy_buckets (source, granularity, bucket_key, energy_kwh)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(source, granularity, bucket_key)
                    DO UPDATE SET energy_kwh = energy_buckets.energy_kwh + excluded.energy_kwh
                    """,
                    (
                        "ble_offpeak" if source == "ble" and granularity.endswith(":offpeak") else source,
                        granularity.split(":", 1)[0],
                        bucket_key,
                        energy_kwh,
                    ),
                )

    def _build_bucket_segments(
        self,
        start: datetime,
        end: datetime,
        average_rate_w_per_min: float,
        *,
        split_offpeak: bool = False,
    ) -> dict[str, dict[str, float]]:
        if not split_offpeak:
            return {
                "hour": self._split_energy_across_buckets(start, end, average_rate_w_per_min, "hour"),
                "day": self._split_energy_across_buckets(start, end, average_rate_w_per_min, "day"),
                "week": self._split_energy_across_buckets(start, end, average_rate_w_per_min, "week"),
                "month": self._split_energy_across_buckets(start, end, average_rate_w_per_min, "month"),
            }
        return {
            "hour": self._split_energy_across_buckets(start, end, average_rate_w_per_min, "hour", offpeak_mode="peak"),
            "day": self._split_energy_across_buckets(start, end, average_rate_w_per_min, "day", offpeak_mode="peak"),
            "week": self._split_energy_across_buckets(start, end, average_rate_w_per_min, "week", offpeak_mode="peak"),
            "month": self._split_energy_across_buckets(start, end, average_rate_w_per_min, "month", offpeak_mode="peak"),
            "hour:offpeak": self._split_energy_across_buckets(start, end, average_rate_w_per_min, "hour", offpeak_mode="offpeak"),
            "day:offpeak": self._split_energy_across_buckets(start, end, average_rate_w_per_min, "day", offpeak_mode="offpeak"),
            "week:offpeak": self._split_energy_across_buckets(start, end, average_rate_w_per_min, "week", offpeak_mode="offpeak"),
            "month:offpeak": self._split_energy_across_buckets(start, end, average_rate_w_per_min, "month", offpeak_mode="offpeak"),
        }

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

    def _split_energy_across_buckets(
        self,
        start: datetime,
        end: datetime,
        average_rate_w_per_min: float,
        granularity: str,
        offpeak_mode: str = "all",
    ) -> dict[str, float]:
        if end <= start:
            return {}

        cursor = start.astimezone(self._timezone)
        end_local = end.astimezone(self._timezone)
        segments: dict[str, float] = {}

        while cursor < end_local:
            bucket_key = self._bucket_key_for_local_datetime(cursor, granularity)
            next_boundary = self._next_bucket_boundary(cursor, granularity)
            offpeak_boundary = self._next_offpeak_boundary(cursor)
            segment_end = min(next_boundary, offpeak_boundary, end_local)
            delta_minutes = (segment_end - cursor).total_seconds() / 60.0
            include_segment = (
                offpeak_mode == "all"
                or (offpeak_mode == "offpeak" and self._is_offpeak(cursor))
                or (offpeak_mode == "peak" and not self._is_offpeak(cursor))
            )
            if delta_minutes > 0 and include_segment:
                segments[bucket_key] = segments.get(bucket_key, 0.0) + (float(average_rate_w_per_min) * delta_minutes) / 1000.0
            cursor = segment_end

        return segments

    def _day_key(self, observed_at: datetime) -> str:
        return observed_at.astimezone(self._timezone).strftime("%Y-%m-%d")

    def _hour_key(self, observed_at: datetime) -> str:
        return observed_at.astimezone(self._timezone).strftime("%Y-%m-%d %H:00")

    def _week_key(self, observed_at: datetime) -> str:
        local = observed_at.astimezone(self._timezone)
        week_start = (local - timedelta(days=local.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
        return week_start.strftime("%Y-%m-%d")

    def _month_key(self, observed_at: datetime) -> str:
        return observed_at.astimezone(self._timezone).strftime("%Y-%m")

    def _bucket_key_for_local_datetime(self, observed_at: datetime, granularity: str) -> str:
        if granularity == "hour":
            return observed_at.strftime("%Y-%m-%d %H:00")
        if granularity == "day":
            return observed_at.strftime("%Y-%m-%d")
        if granularity == "week":
            week_start = (observed_at - timedelta(days=observed_at.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
            return week_start.strftime("%Y-%m-%d")
        if granularity == "month":
            return observed_at.strftime("%Y-%m")
        raise ValueError(f"Unsupported granularity: {granularity}")

    def _next_bucket_boundary(self, observed_at: datetime, granularity: str) -> datetime:
        if granularity == "hour":
            return observed_at.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
        if granularity == "day":
            return observed_at.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
        if granularity == "week":
            week_start = (observed_at - timedelta(days=observed_at.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
            return week_start + timedelta(days=7)
        if granularity == "month":
            month_anchor = observed_at.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            if month_anchor.month == 12:
                return month_anchor.replace(year=month_anchor.year + 1, month=1)
            return month_anchor.replace(month=month_anchor.month + 1)
        raise ValueError(f"Unsupported granularity: {granularity}")

    @staticmethod
    def _is_offpeak(observed_at: datetime) -> bool:
        return 0 <= observed_at.hour < 6

    def _next_offpeak_boundary(self, observed_at: datetime) -> datetime:
        day_start = observed_at.replace(hour=0, minute=0, second=0, microsecond=0)
        offpeak_end = day_start + timedelta(hours=6)
        if observed_at < offpeak_end:
            return offpeak_end
        return day_start + timedelta(days=1)

    def _recent_hour_bucket_keys(self, count: int) -> list[str]:
        now_local = datetime.now(self._timezone).replace(minute=0, second=0, microsecond=0)
        return [(now_local - timedelta(hours=offset)).strftime("%Y-%m-%d %H:00") for offset in range(count - 1, -1, -1)]

    def _recent_day_bucket_keys(self, count: int) -> list[str]:
        now_local = datetime.now(self._timezone).replace(hour=0, minute=0, second=0, microsecond=0)
        return [(now_local - timedelta(days=offset)).strftime("%Y-%m-%d") for offset in range(count - 1, -1, -1)]

    def _recent_week_bucket_keys(self, trailing_days: int) -> list[str]:
        now_local = datetime.now(self._timezone).replace(hour=0, minute=0, second=0, microsecond=0)
        start_local = now_local - timedelta(days=trailing_days - 1)
        first_week_start = (start_local - timedelta(days=start_local.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
        current_week_start = (now_local - timedelta(days=now_local.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
        keys: list[str] = []
        cursor = first_week_start
        while cursor <= current_week_start:
            keys.append(cursor.strftime("%Y-%m-%d"))
            cursor += timedelta(days=7)
        return keys

    def _get_bucket_map(self, connection: sqlite3.Connection, granularity: str, bucket_keys: list[str]) -> dict[str, dict[str, float]]:
        rows = connection.execute(
            f"""
            SELECT source, bucket_key, energy_kwh
            FROM energy_buckets
            WHERE granularity = ?
              AND bucket_key IN ({",".join("?" for _ in bucket_keys)})
            """,
            (granularity, *bucket_keys),
        ).fetchall() if bucket_keys else []

        source_map = {"local_site": "solar", "ble": "grid", "ble_offpeak": "offpeak", "byd_ev": "ev"}
        result = {
            "solar": {bucket_key: 0.0 for bucket_key in bucket_keys},
            "grid": {bucket_key: 0.0 for bucket_key in bucket_keys},
            "offpeak": {bucket_key: 0.0 for bucket_key in bucket_keys},
            "ev": {bucket_key: 0.0 for bucket_key in bucket_keys},
        }
        for row in rows:
            target = source_map.get(row["source"])
            if target:
                result[target][row["bucket_key"]] = float(row["energy_kwh"] or 0.0)
        return result

    def _get_totals_for_key(self, connection: sqlite3.Connection, granularity: str, bucket_key: str) -> dict[str, float]:
        rows = connection.execute(
            """
            SELECT source, energy_kwh
            FROM energy_buckets
            WHERE granularity = ?
              AND bucket_key = ?
            """,
            (granularity, bucket_key),
        ).fetchall()
        totals = {"solar": 0.0, "grid": 0.0, "offpeak": 0.0, "ev": 0.0}
        source_map = {"local_site": "solar", "ble": "grid", "ble_offpeak": "offpeak", "byd_ev": "ev"}
        for row in rows:
            target = source_map.get(row["source"])
            if target:
                totals[target] = float(row["energy_kwh"] or 0.0)
        totals["net"] = totals["solar"] - totals["grid"] - totals["offpeak"]
        return totals

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

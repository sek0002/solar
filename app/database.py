from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from threading import Lock
from typing import Any, Optional


class Database:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = Lock()
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
                    observed_at.astimezone(timezone.utc).isoformat(),
                    grid_usage_watts,
                    solar_generation_watts,
                    payload,
                ),
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
                FROM samples
                WHERE observed_at >= ?
                  AND observed_at <= ?
                ORDER BY observed_at ASC
                LIMIT ?
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

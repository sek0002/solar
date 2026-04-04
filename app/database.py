from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from threading import Lock
from typing import Any


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
        grid_usage_watts: float | None,
        solar_generation_watts: float | None,
        raw_payload: dict[str, Any] | None = None,
    ) -> None:
        payload = json.dumps(raw_payload) if raw_payload is not None else None
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

    def get_recent_samples(self, *, hours: int, limit: int) -> list[dict[str, Any]]:
        since = datetime.now(timezone.utc) - timedelta(hours=hours)
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT source, observed_at, grid_usage_watts, solar_generation_watts, raw_payload
                FROM samples
                WHERE observed_at >= ?
                ORDER BY observed_at ASC
                LIMIT ?
                """,
                (since.isoformat(), limit),
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

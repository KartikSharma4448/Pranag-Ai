# Copyright (c) Kartik Sharma. GitHub: kartiksharma4448
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from universal_index.config import PIPELINE_STATE_PATH, PROCESSED_DIR


class PipelineStateStore:
    def __init__(self, db_path: str | Path = PIPELINE_STATE_PATH) -> None:
        self.db_path = Path(db_path)
        PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._ensure_schema()

    def _connect(self) -> duckdb.DuckDBPyConnection:
        return duckdb.connect(str(self.db_path))

    def _ensure_schema(self) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS ingestion_runs (
                    run_id TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    started_at TIMESTAMP NOT NULL,
                    finished_at TIMESTAMP,
                    metadata_json TEXT,
                    rows_total BIGINT,
                    rows_by_type_json TEXT,
                    notes TEXT
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS source_sync_state (
                    source_name TEXT PRIMARY KEY,
                    last_run_id TEXT,
                    status TEXT NOT NULL,
                    last_completed_at TIMESTAMP,
                    row_count BIGINT,
                    artifact_path TEXT,
                    metadata_json TEXT
                )
                """
            )

    def start_run(self, run_id: str, metadata: dict[str, object] | None = None) -> None:
        started_at = datetime.now(timezone.utc).replace(tzinfo=None)
        with self._connect() as connection:
            connection.execute(
                """
                INSERT OR REPLACE INTO ingestion_runs (
                    run_id, status, started_at, finished_at, metadata_json, rows_total, rows_by_type_json, notes
                )
                VALUES (?, ?, ?, NULL, ?, NULL, NULL, NULL)
                """,
                [run_id, "running", started_at, json.dumps(metadata or {})],
            )

    def mark_source_complete(
        self,
        source_name: str,
        run_id: str,
        row_count: int,
        artifact_path: str | None,
        status: str = "completed",
        metadata: dict[str, object] | None = None,
    ) -> None:
        completed_at = datetime.now(timezone.utc).replace(tzinfo=None)
        with self._connect() as connection:
            connection.execute(
                """
                INSERT OR REPLACE INTO source_sync_state (
                    source_name, last_run_id, status, last_completed_at, row_count, artifact_path, metadata_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    source_name,
                    run_id,
                    status,
                    completed_at,
                    int(row_count),
                    artifact_path,
                    json.dumps(metadata or {}),
                ],
            )

    def finish_run(
        self,
        run_id: str,
        status: str,
        rows_total: int | None = None,
        rows_by_type: dict[str, object] | None = None,
        notes: str | None = None,
    ) -> None:
        finished_at = datetime.now(timezone.utc).replace(tzinfo=None)
        with self._connect() as connection:
            connection.execute(
                """
                UPDATE ingestion_runs
                SET status = ?, finished_at = ?, rows_total = ?, rows_by_type_json = ?, notes = ?
                WHERE run_id = ?
                """,
                [
                    status,
                    finished_at,
                    rows_total,
                    json.dumps(rows_by_type or {}),
                    notes,
                    run_id,
                ],
            )

    def latest_run(self) -> dict[str, object] | None:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT run_id, status, started_at, finished_at, metadata_json, rows_total, rows_by_type_json, notes
                FROM ingestion_runs
                ORDER BY started_at DESC
                LIMIT 1
                """
            ).fetchone()
        if row is None:
            return None
        return {
            "run_id": row[0],
            "status": row[1],
            "started_at": str(row[2]) if row[2] is not None else None,
            "finished_at": str(row[3]) if row[3] is not None else None,
            "metadata": json.loads(row[4] or "{}"),
            "rows_total": row[5],
            "rows_by_type": json.loads(row[6] or "{}"),
            "notes": row[7],
        }

    def source_states(self) -> list[dict[str, object]]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT source_name, last_run_id, status, last_completed_at, row_count, artifact_path, metadata_json
                FROM source_sync_state
                ORDER BY source_name ASC
                """
            ).fetchall()
        return [
            {
                "source_name": row[0],
                "last_run_id": row[1],
                "status": row[2],
                "last_completed_at": str(row[3]) if row[3] is not None else None,
                "row_count": row[4],
                "artifact_path": row[5],
                "metadata": json.loads(row[6] or "{}"),
            }
            for row in rows
        ]

    def summary(self) -> dict[str, object]:
        return {
            "db_path": str(self.db_path),
            "latest_run": self.latest_run(),
            "source_states": self.source_states(),
        }

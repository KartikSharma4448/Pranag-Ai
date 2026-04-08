# Copyright (c) Kartik Sharma. GitHub: kartiksharma4448
from __future__ import annotations

import hashlib
import json
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import duckdb

from universal_index.config import (
    CACHE_BACKEND,
    CACHE_KEY_VERSION,
    PROCESSED_DIR,
    REDIS_KEY_PREFIX,
    REDIS_URL,
    SURROGATE_CACHE_PATH,
)

try:
    import redis
except ImportError:  # pragma: no cover - optional dependency
    redis = None


def make_cache_key(payload: dict[str, object]) -> str:
    versioned_payload = {"cache_key_version": CACHE_KEY_VERSION, **payload}
    normalized = json.dumps(versioned_payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


class DuckDBCache:
    backend_name = "duckdb"

    def __init__(self, db_path: str | Path = SURROGATE_CACHE_PATH) -> None:
        self.db_path = Path(db_path)
        PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._ensure_schema()

    def _ensure_schema(self) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS surrogate_cache (
                    cache_key TEXT PRIMARY KEY,
                    cache_type TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    created_at TIMESTAMP NOT NULL,
                    expires_at TIMESTAMP NOT NULL
                )
                """
            )

    def _connect(self) -> duckdb.DuckDBPyConnection:
        last_error: Exception | None = None
        for _ in range(10):
            try:
                return duckdb.connect(str(self.db_path))
            except duckdb.IOException as error:
                last_error = error
                time.sleep(0.2)
        if last_error is not None:
            raise last_error
        raise RuntimeError("Failed to connect to DuckDB cache.")

    def get(self, cache_type: str, cache_key: str) -> dict[str, object] | None:
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT payload_json
                FROM surrogate_cache
                WHERE cache_key = ? AND cache_type = ? AND expires_at > ?
                """,
                [cache_key, cache_type, now],
            ).fetchone()
        if row is None:
            return None
        return json.loads(row[0])

    def set(
        self,
        cache_type: str,
        cache_key: str,
        payload: dict[str, object],
        ttl_seconds: int,
    ) -> None:
        created_at = datetime.now(timezone.utc).replace(tzinfo=None)
        expires_at = created_at + timedelta(seconds=ttl_seconds)
        payload_json = json.dumps(payload)
        with self._connect() as connection:
            connection.execute(
                """
                INSERT OR REPLACE INTO surrogate_cache (
                    cache_key, cache_type, payload_json, created_at, expires_at
                )
                VALUES (?, ?, ?, ?, ?)
                """,
                [cache_key, cache_type, payload_json, created_at, expires_at],
            )

    def stats(self) -> dict[str, object]:
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        with self._connect() as connection:
            total_rows = connection.execute(
                "SELECT COUNT(*) FROM surrogate_cache"
            ).fetchone()[0]
            active_rows = connection.execute(
                "SELECT COUNT(*) FROM surrogate_cache WHERE expires_at > ?",
                [now],
            ).fetchone()[0]
        return {
            "backend": self.backend_name,
            "db_path": str(self.db_path),
            "rows_total": int(total_rows),
            "rows_active": int(active_rows),
        }

    def publish_event(self, stream_key: str, payload: dict[str, object]) -> None:
        return None


class RedisCache:
    backend_name = "redis"

    def __init__(self, redis_url: str = REDIS_URL, prefix: str = REDIS_KEY_PREFIX) -> None:
        if redis is None:
            raise RuntimeError("Redis backend requested but `redis` package is not installed.")
        self.redis_url = redis_url
        self.prefix = prefix
        self.client = redis.Redis.from_url(redis_url, decode_responses=True)
        self.client.ping()

    def _make_key(self, cache_type: str, cache_key: str) -> str:
        return f"{self.prefix}:cache:{cache_type}:{cache_key}"

    def get(self, cache_type: str, cache_key: str) -> dict[str, object] | None:
        payload = self.client.get(self._make_key(cache_type, cache_key))
        if payload is None:
            return None
        return json.loads(payload)

    def set(
        self,
        cache_type: str,
        cache_key: str,
        payload: dict[str, object],
        ttl_seconds: int,
    ) -> None:
        self.client.set(
            self._make_key(cache_type, cache_key),
            json.dumps(payload),
            ex=ttl_seconds,
        )

    def stats(self) -> dict[str, object]:
        active_rows = 0
        cursor = 0
        pattern = f"{self.prefix}:cache:*"
        while True:
            cursor, keys = self.client.scan(cursor=cursor, match=pattern, count=200)
            active_rows += len(keys)
            if cursor == 0:
                break
        return {
            "backend": self.backend_name,
            "redis_url": self.redis_url,
            "key_prefix": self.prefix,
            "rows_total": active_rows,
            "rows_active": active_rows,
        }

    def publish_event(self, stream_key: str, payload: dict[str, object]) -> None:
        stream_name = (
            stream_key if stream_key.startswith(f"{self.prefix}:") else f"{self.prefix}:{stream_key}"
        )
        serializable = {key: json.dumps(value) for key, value in payload.items()}
        self.client.xadd(stream_name, serializable, maxlen=2000, approximate=True)


def build_cache_backend(preferred_backend: str = CACHE_BACKEND):
    preferred = preferred_backend.strip().lower()
    if preferred == "redis":
        try:
            return RedisCache()
        except Exception:
            return DuckDBCache()
    return DuckDBCache()

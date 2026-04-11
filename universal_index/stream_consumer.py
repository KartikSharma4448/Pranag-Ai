from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timezone

from universal_index.cache import RedisCache
from universal_index.config import PROCESSED_DIR, REDIS_STREAM_KEY

CONSUMER_SUMMARY_PATH = PROCESSED_DIR / "stream_consumer_summary.json"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Consume ingestion events from Redis Streams.")
    parser.add_argument("--stream-key", default=REDIS_STREAM_KEY)
    parser.add_argument("--count", type=int, default=100)
    parser.add_argument("--block-ms", type=int, default=5000)
    parser.add_argument("--once", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    cache = RedisCache()

    stream_name = (
        args.stream_key
        if args.stream_key.startswith(f"{cache.prefix}:")
        else f"{cache.prefix}:{args.stream_key}"
    )

    last_id = "$"
    counters: dict[str, int] = {}

    while True:
        rows = cache.client.xread({stream_name: last_id}, count=args.count, block=args.block_ms)
        if not rows:
            if args.once:
                break
            continue

        for _, messages in rows:
            for message_id, fields in messages:
                last_id = message_id
                event_name = _decode_event_name(fields)
                counters[event_name] = counters.get(event_name, 0) + 1

        payload = {
            "captured_at_utc": datetime.now(timezone.utc).isoformat(),
            "stream": stream_name,
            "last_id": last_id,
            "events": counters,
        }
        PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
        CONSUMER_SUMMARY_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")

        if args.once:
            break

        time.sleep(0.5)



def _decode_event_name(fields: dict[str, str]) -> str:
    raw = fields.get("event")
    if raw is None:
        return "unknown"
    try:
        return str(json.loads(raw))
    except Exception:
        return str(raw)


if __name__ == "__main__":
    main()

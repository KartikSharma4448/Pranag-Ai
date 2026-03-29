# Copyright (c) Kartik Sharma. GitHub: kartiksharma4448
from __future__ import annotations

import argparse
import subprocess
import sys
import time

from universal_index.config import SCHEDULER_INTERVAL_MINUTES


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Simple interval scheduler for distributed ingestion runs.")
    parser.add_argument("--interval-minutes", type=int, default=SCHEDULER_INTERVAL_MINUTES)
    parser.add_argument("--include-literature", action="store_true")
    parser.add_argument("--refresh-vectors", action="store_true")
    parser.add_argument("--once", action="store_true")
    return parser.parse_args()


def run_ingestion(args: argparse.Namespace) -> int:
    command = [sys.executable, "-m", "universal_index.distributed_ingest"]
    if args.include_literature:
        command.append("--include-literature")
    if args.refresh_vectors:
        command.append("--refresh-vectors")
    return subprocess.run(command, check=False).returncode


def main() -> None:
    args = parse_args()
    while True:
        exit_code = run_ingestion(args)
        if args.once:
            if exit_code != 0:
                raise SystemExit(exit_code)
            return
        time.sleep(max(int(args.interval_minutes), 1) * 60)


if __name__ == "__main__":
    main()

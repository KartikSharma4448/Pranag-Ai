# Copyright (c) Kartik Sharma. GitHub: kartiksharma4448
from __future__ import annotations

import argparse
import subprocess
import sys
import threading
import time

from universal_index.config import SCHEDULER_FEEDS_INTERVAL_MINUTES, SCHEDULER_INTERVAL_MINUTES


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scheduler for distributed ingestion and feeds loading.")
    parser.add_argument("--interval-minutes", type=int, default=SCHEDULER_INTERVAL_MINUTES)
    parser.add_argument("--feeds-interval-minutes", type=int, default=SCHEDULER_FEEDS_INTERVAL_MINUTES)
    parser.add_argument("--include-literature", action="store_true")
    parser.add_argument("--refresh-vectors", action="store_true")
    parser.add_argument("--feeds-only", action="store_true", help="Run only feeds loading job")
    parser.add_argument("--ingestion-only", action="store_true", help="Run only ingestion job")
    parser.add_argument("--once", action="store_true")
    return parser.parse_args()


def run_ingestion(args: argparse.Namespace) -> int:
    """Run distributed ingestion job."""
    command = [sys.executable, "-m", "universal_index.distributed_ingest"]
    if args.include_literature:
        command.append("--include-literature")
    if args.refresh_vectors:
        command.append("--refresh-vectors")
    return subprocess.run(command, check=False).returncode


def run_feeds(args: argparse.Namespace) -> int:
    """Run feeds loading job."""
    command = [sys.executable, "-m", "universal_index.distributed_ingest", "--load-feeds-only"]
    return subprocess.run(command, check=False).returncode


def schedule_job(
    job_name: str,
    job_func,
    interval_minutes: int,
    args: argparse.Namespace,
    once: bool = False,
) -> None:
    """Schedule a job to run at regular intervals."""
    while True:
        try:
            print(f"[{job_name}] Starting job...")
            exit_code = job_func(args)
            if exit_code != 0:
                print(f"[{job_name}] Job failed with exit code {exit_code}")
            else:
                print(f"[{job_name}] Job completed successfully")
        except Exception as error:
            print(f"[{job_name}] Job error: {error}")
        
        if once:
            return
        
        print(f"[{job_name}] Sleeping for {interval_minutes} minutes...")
        time.sleep(max(int(interval_minutes), 1) * 60)


def main() -> None:
    args = parse_args()
    
    # Determine which jobs to run
    run_ingestion_job = not args.feeds_only
    run_feeds_job = not args.ingestion_only
    
    threads = []
    
    if run_ingestion_job:
        ingestion_thread = threading.Thread(
            target=schedule_job,
            args=("Ingestion", run_ingestion, args.interval_minutes, args, args.once),
            daemon=True,
        )
        threads.append(ingestion_thread)
        ingestion_thread.start()
    
    if run_feeds_job:
        feeds_thread = threading.Thread(
            target=schedule_job,
            args=("Feeds", run_feeds, args.feeds_interval_minutes, args, args.once),
            daemon=True,
        )
        threads.append(feeds_thread)
        feeds_thread.start()
    
    if not threads:
        print("No jobs to run. Use --help for options.")
        return
    
    # Wait for all threads to complete
    for thread in threads:
        thread.join()


if __name__ == "__main__":
    main()


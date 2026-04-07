from __future__ import annotations

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent


def run_step(name: str, command: list[str]) -> int:
    print(f"\n[STEP] {name}")
    print("[CMD]", " ".join(command))
    return subprocess.run(command, cwd=ROOT, check=False).returncode


def main() -> int:
    python = sys.executable

    menu = {
        "1": ("Build universal index", [python, "-m", "universal_index.build"]),
        "2": ("Build vector index", [python, "-m", "universal_index.vector_search"]),
        "3": (
            "Run distributed ingestion",
            [python, "-m", "universal_index.distributed_ingest", "--include-literature", "--refresh-vectors"],
        ),
        "4": ("Start API server", [python, "-m", "uvicorn", "api.main:app", "--reload"]),
        "5": ("Run tests", [python, "-m", "unittest", "discover", "-s", "tests", "-p", "test_*.py"]),
    }

    print("PRANA-G CLI Launcher")
    print("Choose an option:")
    for key, (title, _) in menu.items():
        print(f"  {key}. {title}")
    print("  0. Exit")

    choice = input("Enter choice: ").strip()
    if choice == "0":
        print("Exit.")
        return 0

    selected = menu.get(choice)
    if selected is None:
        print("Invalid option.")
        return 1

    title, command = selected
    return run_step(title, command)


if __name__ == "__main__":
    raise SystemExit(main())

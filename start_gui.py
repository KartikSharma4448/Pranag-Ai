from __future__ import annotations

import subprocess
import sys
import threading
import tkinter as tk
from pathlib import Path
from tkinter import messagebox

ROOT = Path(__file__).resolve().parent


class PranagGui(tk.Tk):
    def __init__(self) -> None:
        super().__init__()
        self.title("PRANA-G GUI Launcher")
        self.geometry("720x430")

        self.python = sys.executable

        self.log = tk.Text(self, height=16, width=95)
        self.log.pack(padx=10, pady=10)

        button_frame = tk.Frame(self)
        button_frame.pack(padx=10, pady=6)

        tk.Button(button_frame, text="Build Index", width=18, command=self.build_index).grid(row=0, column=0, padx=5, pady=5)
        tk.Button(button_frame, text="Build Vectors", width=18, command=self.build_vectors).grid(row=0, column=1, padx=5, pady=5)
        tk.Button(button_frame, text="Run Ingestion", width=18, command=self.run_ingestion).grid(row=0, column=2, padx=5, pady=5)
        tk.Button(button_frame, text="Run Tests", width=18, command=self.run_tests).grid(row=1, column=0, padx=5, pady=5)
        tk.Button(button_frame, text="Start API", width=18, command=self.start_api).grid(row=1, column=1, padx=5, pady=5)
        tk.Button(button_frame, text="Open Docs", width=18, command=self.open_docs).grid(row=1, column=2, padx=5, pady=5)
        tk.Button(button_frame, text="Run Benchmark", width=18, command=self.run_benchmark).grid(row=2, column=0, padx=5, pady=5)

        self.append_log("GUI ready. Choose an action.\n")

    def append_log(self, text: str) -> None:
        self.log.insert(tk.END, text)
        self.log.see(tk.END)

    def run_cmd_async(self, title: str, cmd: list[str]) -> None:
        def worker() -> None:
            self.append_log(f"\n[STEP] {title}\n")
            self.append_log(f"[CMD] {' '.join(cmd)}\n")
            proc = subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True)
            if proc.stdout:
                self.append_log(proc.stdout + "\n")
            if proc.stderr:
                self.append_log(proc.stderr + "\n")
            self.append_log(f"[EXIT] {proc.returncode}\n")

        threading.Thread(target=worker, daemon=True).start()

    def build_index(self) -> None:
        self.run_cmd_async("Build universal index", [self.python, "-m", "universal_index.build"])

    def build_vectors(self) -> None:
        self.run_cmd_async("Build vector index", [self.python, "-m", "universal_index.vector_search"])

    def run_ingestion(self) -> None:
        self.run_cmd_async(
            "Run distributed ingestion",
            [self.python, "-m", "universal_index.distributed_ingest", "--include-literature", "--refresh-vectors"],
        )

    def run_tests(self) -> None:
        self.run_cmd_async("Run tests", [self.python, "-m", "unittest", "discover", "-s", "tests", "-p", "test_*.py"])

    def start_api(self) -> None:
        self.run_cmd_async("Start API server", [self.python, "-m", "uvicorn", "api.main:app", "--reload"])

    def open_docs(self) -> None:
        import webbrowser

        ok = webbrowser.open("http://127.0.0.1:8000/docs")
        if not ok:
            messagebox.showinfo("Info", "Could not open browser automatically. Open http://127.0.0.1:8000/docs")

    def run_benchmark(self) -> None:
        self.run_cmd_async("Run benchmark", [self.python, "-m", "universal_index.benchmark"])


def main() -> int:
    app = PranagGui()
    app.mainloop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

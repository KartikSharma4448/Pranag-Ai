# Copyright (c) Kartik Sharma. GitHub: kartiksharma4448
from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from universal_index.state import PipelineStateStore


class PipelineStateStoreTests(unittest.TestCase):
    def test_run_and_source_state_are_persisted(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "pipeline_state.duckdb"
            store = PipelineStateStore(db_path=db_path)

            store.start_run("run-001", metadata={"refresh_vectors": True})
            store.mark_source_complete(
                source_name="genes",
                run_id="run-001",
                row_count=500,
                artifact_path="data/lake/source=genes/run_id=run-001/part-000.parquet",
            )
            store.finish_run(
                run_id="run-001",
                status="completed",
                rows_total=500,
                rows_by_type={"gene": 500},
            )

            latest = store.latest_run()
            self.assertIsNotNone(latest)
            self.assertEqual(latest["run_id"], "run-001")
            self.assertEqual(latest["status"], "completed")
            self.assertEqual(latest["rows_total"], 500)

            sources = store.source_states()
            self.assertEqual(len(sources), 1)
            self.assertEqual(sources[0]["source_name"], "genes")
            self.assertEqual(sources[0]["row_count"], 500)


if __name__ == "__main__":
    unittest.main()

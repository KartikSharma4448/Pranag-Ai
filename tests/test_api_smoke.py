# Copyright (c) Kartik Sharma. GitHub: kartiksharma4448
from __future__ import annotations

import unittest

from fastapi.testclient import TestClient

from api.main import app
from universal_index.config import (
    CHROMA_DIR,
    INGESTION_SUMMARY_PATH,
    LITERATURE_SUMMARY_PATH,
    PARQUET_PATH,
)


@unittest.skipUnless(PARQUET_PATH.exists(), "Run `python -m universal_index.build` first.")
@unittest.skipUnless(CHROMA_DIR.exists(), "Run `python -m universal_index.vector_search` first.")
class ApiSmokeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.client = TestClient(app)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.client.close()

    def test_health_endpoint_reports_ready_state(self) -> None:
        response = self.client.get("/health")
        self.assertEqual(response.status_code, 200)

        payload = response.json()
        self.assertEqual(payload["status"], "ok")
        self.assertTrue(payload["parquet_ready"])
        self.assertTrue(payload["vector_ready"])
        self.assertIn("cache", payload)

    def test_context_endpoint_returns_nested_payload(self) -> None:
        response = self.client.get("/context", params={"lat": 26.3, "lon": 73.0})
        self.assertEqual(response.status_code, 200)

        payload = response.json()
        self.assertEqual(payload["location_name"], "Jodhpur Rajasthan")
        self.assertIn("soil", payload)
        self.assertIn("climate", payload)
        self.assertIn("agriculture", payload)
        self.assertIn("providers", payload)
        self.assertEqual(payload["soil"]["type"], "sandy_loam")

    def test_search_endpoint_returns_mixed_domains(self) -> None:
        response = self.client.get(
            "/search",
            params={"q": "self healing high temperature material", "top_k": 4},
        )
        self.assertEqual(response.status_code, 200)

        payload = response.json()
        self.assertEqual(payload["rows"], 4)
        entity_types = {item["entity_type"] for item in payload["items"]}
        self.assertGreaterEqual(len(entity_types), 3)

    def test_recommend_endpoint_returns_combination(self) -> None:
        response = self.client.get(
            "/recommend",
            params={
                "q": "Design a self healing high temperature material for Rajasthan desert deployment",
                "lat": 26.3,
                "lon": 73.0,
                "top_k": 6,
            },
        )
        self.assertEqual(response.status_code, 200)

        payload = response.json()
        self.assertIn("recommended_combination", payload)
        self.assertIn("final_recommendations", payload)
        self.assertGreaterEqual(len(payload["final_recommendations"]), 3)

        recommended_types = set(payload["recommended_combination"].keys())
        self.assertIn("material", recommended_types)
        self.assertIn("molecule", recommended_types)
        self.assertIn("gene", recommended_types)

    def test_literature_status_endpoint_returns_summary(self) -> None:
        if not LITERATURE_SUMMARY_PATH.exists():
            self.skipTest("Run `python -m universal_index.literature_agent` first.")
        response = self.client.get("/literature/status")
        self.assertEqual(response.status_code, 200)

        payload = response.json()
        self.assertIn("papers_fetched", payload)
        self.assertIn("entities_extracted", payload)

    def test_ingestion_status_endpoint_returns_summary(self) -> None:
        if not INGESTION_SUMMARY_PATH.exists():
            self.skipTest("Run `python -m universal_index.distributed_ingest` first.")
        response = self.client.get("/ingestion/status")
        self.assertEqual(response.status_code, 200)

        payload = response.json()
        self.assertIn("run_id", payload)
        self.assertIn("rows_total", payload)


if __name__ == "__main__":
    unittest.main()

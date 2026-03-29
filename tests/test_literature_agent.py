# Copyright (c) Kartik Sharma. GitHub: kartiksharma4448
from __future__ import annotations

import unittest

import pandas as pd

from universal_index.literature_agent import extract_entities_from_papers


class LiteratureAgentTests(unittest.TestCase):
    def test_extract_entities_from_cross_domain_paper(self) -> None:
        papers = pd.DataFrame(
            [
                {
                    "paper_id": "paper-001",
                    "paper_source": "PubMed",
                    "title": "Bacillus repair genes guide epoxy composite coating design",
                    "abstract": (
                        "The study links HSP70 stress response, epoxy molecule healing, "
                        "perovskite oxide coatings, and heat transfer simulation at 48 C."
                    ),
                    "journal": "Test Journal",
                    "published": "2026",
                    "url": "https://example.org/paper-001",
                }
            ]
        )

        entities = extract_entities_from_papers(papers)

        self.assertFalse(entities.empty)
        entity_types = set(entities["entity_type"].tolist())
        self.assertIn("gene", entity_types)
        self.assertIn("molecule", entity_types)
        self.assertIn("material", entity_types)
        self.assertIn("simulation", entity_types)


if __name__ == "__main__":
    unittest.main()

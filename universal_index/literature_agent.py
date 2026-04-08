# Copyright (c) Kartik Sharma. GitHub: kartiksharma4448
from __future__ import annotations

import argparse
import json
import re
import xml.etree.ElementTree as ET
from pathlib import Path
from urllib.parse import urlencode

import pandas as pd
import requests

from universal_index.build import run_duckdb_validation
from universal_index.config import (
    BUILD_SUMMARY_PATH,
    CHROMA_COLLECTION_NAME,
    CHROMA_DIR,
    DUCKDB_PATH,
    EMBEDDING_MODEL_NAME,
    ENTREZ_EMAIL,
    LITERATURE_LLM_API_KEY,
    LITERATURE_LLM_ENABLED,
    LITERATURE_LLM_ENDPOINT,
    LITERATURE_LLM_MODEL,
    LITERATURE_ENTITY_PATH,
    LITERATURE_PAPERS_RAW_PATH,
    LITERATURE_SUMMARY_PATH,
    NCBI_API_KEY,
    PARQUET_PATH,
    PROCESSED_DIR,
    RAW_DIR,
    VALIDATION_CSV_PATH,
)
from universal_index.schema import concat_frames, normalize_frame

NCBI_BASE_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"
ARXIV_API_URL = "https://export.arxiv.org/api/query"
CROSSREF_WORKS_URL = "https://api.crossref.org/works"
REQUEST_TIMEOUT_SECONDS = 30
USER_AGENT = "universal-index-literature-agent/0.1"
GENE_PATTERN = re.compile(r"\b[A-Z][A-Z0-9-]{2,11}\b")
FORMULA_PATTERN = re.compile(r"\b(?:[A-Z][a-z]?\d{0,3}){2,}\b")
TEMPERATURE_PATTERN = re.compile(r"(\d+(?:\.\d+)?)\s*(?:°\s*C|C)\b", re.IGNORECASE)
STRENGTH_PATTERN = re.compile(r"(\d+(?:\.\d+)?)\s*(?:GPa|MPa)\b", re.IGNORECASE)
CONDUCTIVITY_PATTERN = re.compile(
    r"(\d+(?:\.\d+)?)\s*(?:S/cm|S m-1|W/mK|W m-1 K-1)\b",
    re.IGNORECASE,
)
BIO_KEYWORDS = [
    "gene",
    "protein",
    "pathway",
    "enzyme",
    "cell",
    "bacteria",
    "biomineralization",
    "expression",
]
MATERIAL_KEYWORDS = [
    "material",
    "alloy",
    "ceramic",
    "oxide",
    "composite",
    "semiconductor",
    "perovskite",
    "film",
    "coating",
    "crystal",
]
MOLECULE_KEYWORDS = [
    "molecule",
    "compound",
    "ligand",
    "inhibitor",
    "amine",
    "acid",
    "epoxy",
    "polymer",
    "peptide",
    "solvent",
]
SIMULATION_KEYWORDS = [
    "simulation",
    "model",
    "cfd",
    "fea",
    "finite element",
    "heat transfer",
    "thermal expansion",
    "flow",
    "radiation",
]
GENE_BLACKLIST = {
    "DNA",
    "RNA",
    "ATP",
    "NMR",
    "COVID",
    "CELL",
    "NASA",
    "NIST",
    "FEA",
    "CFD",
}
PAPER_RELEVANCE_KEYWORDS = sorted(
    set(BIO_KEYWORDS + MATERIAL_KEYWORDS + MOLECULE_KEYWORDS + SIMULATION_KEYWORDS)
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch a small batch of recent papers and append extracted entities."
    )
    parser.add_argument("--pubmed", type=int, default=6)
    parser.add_argument("--arxiv", type=int, default=6)
    parser.add_argument("--entrez-email", default=ENTREZ_EMAIL)
    parser.add_argument("--ncbi-api-key", default=NCBI_API_KEY)
    parser.add_argument("--skip-vector-refresh", action="store_true")
    return parser.parse_args()


def build_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})
    return session


def fetch_pubmed_papers(
    sample_size: int, email: str, api_key: str | None = None
) -> pd.DataFrame:
    session = build_session()
    search_response = session.get(
        f"{NCBI_BASE_URL}/esearch.fcgi",
        params={
            "db": "pubmed",
            "term": (
                "(biomineralization OR biomaterial OR polymer OR semiconductor OR catalyst "
                "OR thermal material OR self-healing material OR heat transfer)"
            ),
            "sort": "pub_date",
            "retmax": sample_size,
            "retmode": "json",
            "email": email,
            "api_key": api_key,
        },
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    search_response.raise_for_status()
    payload = search_response.json()
    ids = payload.get("esearchresult", {}).get("idlist", [])
    if not ids:
        raise RuntimeError("PubMed did not return any paper identifiers.")

    fetch_response = session.get(
        f"{NCBI_BASE_URL}/efetch.fcgi",
        params={
            "db": "pubmed",
            "id": ",".join(ids),
            "retmode": "xml",
            "email": email,
            "api_key": api_key,
        },
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    fetch_response.raise_for_status()

    root = ET.fromstring(fetch_response.text)
    papers: list[dict[str, object]] = []
    for article in root.findall(".//PubmedArticle"):
        pmid = _extract_text(article.find(".//PMID"))
        title = _extract_text(article.find(".//ArticleTitle"))
        abstract_parts = [
            _extract_text(node)
            for node in article.findall(".//Abstract/AbstractText")
            if _extract_text(node)
        ]
        abstract = " ".join(abstract_parts).strip()
        journal = _extract_text(article.find(".//Journal/Title"))
        year = _extract_text(article.find(".//PubDate/Year"))
        if not pmid or not title:
            continue
        papers.append(
            {
                "paper_id": f"pubmed-{pmid}",
                "paper_source": "PubMed",
                "title": title,
                "abstract": abstract,
                "journal": journal,
                "published": year,
                "url": f"https://pubmed.ncbi.nlm.nih.gov/{pmid}/",
            }
        )

    if not papers:
        raise RuntimeError("PubMed fetch succeeded but no article records were parsed.")

    return pd.DataFrame(papers)


def fetch_arxiv_papers(sample_size: int) -> pd.DataFrame:
    session = build_session()
    query = {
        "search_query": "all:biomaterial OR all:polymer OR all:semiconductor OR all:thermal",
        "start": 0,
        "max_results": sample_size,
        "sortBy": "submittedDate",
        "sortOrder": "descending",
    }
    response = session.get(
        f"{ARXIV_API_URL}?{urlencode(query)}",
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    response.raise_for_status()

    namespace = {"atom": "http://www.w3.org/2005/Atom"}
    root = ET.fromstring(response.text)
    papers: list[dict[str, object]] = []
    for entry in root.findall("atom:entry", namespace):
        paper_id = _extract_text(entry.find("atom:id", namespace))
        title = _extract_text(entry.find("atom:title", namespace))
        abstract = _extract_text(entry.find("atom:summary", namespace))
        published = _extract_text(entry.find("atom:published", namespace))
        if not paper_id or not title:
            continue
        identifier = paper_id.rsplit("/", maxsplit=1)[-1]
        papers.append(
            {
                "paper_id": f"arxiv-{identifier}",
                "paper_source": "arXiv",
                "title": title,
                "abstract": abstract,
                "journal": "arXiv",
                "published": published,
                "url": paper_id,
            }
        )

    if not papers:
        raise RuntimeError("arXiv did not return any papers.")

    return pd.DataFrame(papers)


def fetch_crossref_journal_papers(journal_title: str, sample_size: int) -> pd.DataFrame:
    session = build_session()
    response = session.get(
        CROSSREF_WORKS_URL,
        params={
            "query.container-title": journal_title,
            "rows": sample_size,
            "select": "DOI,title,container-title,issued,URL,abstract",
            "sort": "published",
            "order": "desc",
        },
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    payload = response.json()
    items = payload.get("message", {}).get("items", []) if isinstance(payload, dict) else []

    papers: list[dict[str, object]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        title = _first_text(item.get("title"))
        doi = str(item.get("DOI") or "").strip()
        if not title or not doi:
            continue
        year = _crossref_year(item.get("issued"))
        abstract = _clean_abstract(item.get("abstract"))
        papers.append(
            {
                "paper_id": f"crossref-{_slugify(doi)}",
                "paper_source": journal_title,
                "title": title,
                "abstract": abstract,
                "journal": journal_title,
                "published": year,
                "url": str(item.get("URL") or f"https://doi.org/{doi}"),
            }
        )

    if not papers:
        raise RuntimeError(f"Crossref did not return any papers for {journal_title}.")

    return pd.DataFrame(papers)


def generate_paper_fallback(source_name: str, sample_size: int) -> pd.DataFrame:
    records = [
        {
            "paper_id": f"{source_name.lower()}-fallback-{index + 1:03d}",
            "paper_source": source_name,
            "title": (
                "High-temperature self-healing ceramic-polymer composites for desert deployment "
                f"benchmark {index + 1}"
            ),
            "abstract": (
                "This fallback paper discusses Bacillus biomineralization, epoxy self-healing "
                "chemistry, perovskite oxide coatings, and thermal expansion simulation at 48 C."
            ),
            "journal": source_name,
            "published": "2026",
            "url": f"https://example.org/{source_name.lower()}/{index + 1}",
        }
        for index in range(sample_size)
    ]
    return pd.DataFrame(records)


def _first_text(value: object) -> str:
    if isinstance(value, list) and value:
        return str(value[0]).strip()
    if value is None:
        return ""
    return str(value).strip()


def _crossref_year(value: object) -> str:
    if not isinstance(value, dict):
        return ""
    date_parts = value.get("date-parts")
    if isinstance(date_parts, list) and date_parts:
        first = date_parts[0]
        if isinstance(first, list) and first:
            return str(first[0])
    return ""


def _clean_abstract(value: object) -> str:
    if value is None:
        return ""
    text = str(value)
    text = re.sub(r"<[^>]+>", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _slugify(value: str) -> str:
    return re.sub(r"[^a-zA-Z0-9]+", "-", value.lower()).strip("-") or "paper"


def select_relevant_papers(
    papers: pd.DataFrame, target_count: int, min_score: int = 2
) -> pd.DataFrame:
    if papers.empty:
        return papers

    working = papers.copy()
    working["relevance_score"] = working.apply(score_paper_relevance, axis=1)
    working = working[working["relevance_score"] >= min_score]
    working = working.sort_values(
        by=["relevance_score", "published"],
        ascending=[False, False],
        na_position="last",
    ).reset_index(drop=True)
    return working.head(target_count).drop(columns=["relevance_score"], errors="ignore")


def score_paper_relevance(row: pd.Series) -> int:
    text = " ".join(
        str(row.get(column) or "") for column in ["title", "abstract", "journal"]
    ).lower()
    return sum(1 for keyword in PAPER_RELEVANCE_KEYWORDS if keyword in text)


def extract_entities_from_papers(papers: pd.DataFrame) -> pd.DataFrame:
    extracted: list[dict[str, object]] = []
    for paper in papers.to_dict(orient="records"):
        extracted.extend(extract_entities_from_paper(paper))
    if not extracted:
        return normalize_frame(pd.DataFrame())
    frame = normalize_frame(pd.DataFrame(extracted))
    return frame.drop_duplicates(subset=["entity_id"]).reset_index(drop=True)


def extract_entities_from_paper(paper: dict[str, object]) -> list[dict[str, object]]:
    title = str(paper.get("title") or "").strip()
    abstract = str(paper.get("abstract") or "").strip()
    combined_text = " ".join(part for part in [title, abstract] if part).strip()
    combined_lower = combined_text.lower()
    entities: list[dict[str, object]] = _llm_extract_entities(paper) or []

    paper_id = str(paper.get("paper_id") or "paper-unknown")
    source_name = str(paper.get("paper_source") or "Literature")

    if _contains_any(combined_lower, BIO_KEYWORDS):
        gene_name = extract_gene_name(combined_text) or _truncate_title(title, 7)
        entities.append(
            _make_entity(
                paper_id=paper_id,
                entity_type="gene",
                name=gene_name,
                description=f"Literature-derived biological signal. Title: {title}. Abstract: {abstract}",
                source=f"{source_name} literature agent",
                combined_text=combined_text,
            )
        )

    if _contains_any(combined_lower, MATERIAL_KEYWORDS) or FORMULA_PATTERN.search(combined_text):
        material_name = extract_material_name(combined_text, title) or _truncate_title(title, 8)
        entities.append(
            _make_entity(
                paper_id=paper_id,
                entity_type="material",
                name=material_name,
                description=f"Literature-derived materials candidate. Title: {title}. Abstract: {abstract}",
                source=f"{source_name} literature agent",
                combined_text=combined_text,
            )
        )

    if _contains_any(combined_lower, MOLECULE_KEYWORDS):
        molecule_name = extract_molecule_name(combined_text, title) or _truncate_title(title, 8)
        entities.append(
            _make_entity(
                paper_id=paper_id,
                entity_type="molecule",
                name=molecule_name,
                description=f"Literature-derived molecular candidate. Title: {title}. Abstract: {abstract}",
                source=f"{source_name} literature agent",
                combined_text=combined_text,
            )
        )

    if _contains_any(combined_lower, SIMULATION_KEYWORDS):
        entities.append(
            _make_entity(
                paper_id=paper_id,
                entity_type="simulation",
                name=f"Simulation insight from {_truncate_title(title, 6)}",
                description=f"Literature-derived engineering simulation signal. Title: {title}. Abstract: {abstract}",
                source=f"{source_name} literature agent",
                combined_text=combined_text,
            )
        )

    if not entities:
        inferred_type = infer_primary_entity_type(combined_lower)
        entities.append(
            _make_entity(
                paper_id=paper_id,
                entity_type=inferred_type,
                name=_truncate_title(title, 8),
                description=f"Literature-derived scientific entity. Title: {title}. Abstract: {abstract}",
                source=f"{source_name} literature agent",
                combined_text=combined_text,
            )
        )

    return entities


def _llm_extract_entities(paper: dict[str, object]) -> list[dict[str, object]] | None:
    if not LITERATURE_LLM_ENABLED or not LITERATURE_LLM_ENDPOINT:
        return None

    title = str(paper.get("title") or "").strip()
    abstract = str(paper.get("abstract") or "").strip()
    payload = {
        "model": LITERATURE_LLM_MODEL,
        "messages": [
            {
                "role": "system",
                "content": (
                    "Extract scientific entities as strict JSON only. "
                    "Return an array of objects with keys: entity_type, name, description, temperature_max, strength, conductivity. "
                    "Valid entity_type values: gene, material, molecule, simulation, protein, structure."
                ),
            },
            {
                "role": "user",
                "content": f"Title: {title}\nAbstract: {abstract}",
            },
        ],
        "temperature": 0,
    }
    headers = {"Content-Type": "application/json"}
    if LITERATURE_LLM_API_KEY:
        headers["Authorization"] = f"Bearer {LITERATURE_LLM_API_KEY}"

    try:
        response = requests.post(
            LITERATURE_LLM_ENDPOINT,
            headers=headers,
            json=payload,
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        result = response.json()
    except Exception:
        return None

    parsed = _parse_llm_response(result)
    if not parsed:
        return None

    paper_id = str(paper.get("paper_id") or "paper-unknown")
    source_name = str(paper.get("paper_source") or "Literature")
    extracted = [
        _normalize_llm_entity(item, paper_id=paper_id, source_name=source_name, default_name=title)
        for item in parsed
    ]
    return [item for item in extracted if item is not None] or None


def _extract_llm_content(payload: object) -> str:
    if not isinstance(payload, dict):
        return ""
    choices = payload.get("choices", [])
    if isinstance(choices, list) and choices:
        first = choices[0]
        if isinstance(first, dict):
            message = first.get("message", {})
            if isinstance(message, dict):
                content = message.get("content")
                if content is not None:
                    return str(content).strip()
    return ""


def _parse_llm_response(payload: object) -> list[dict[str, object]] | None:
    content = _extract_llm_content(payload)
    if not content:
        return None
    try:
        parsed = json.loads(content)
    except Exception:
        return None
    if not isinstance(parsed, list):
        return None
    return [item for item in parsed if isinstance(item, dict)]


def _normalize_llm_entity(
    item: dict[str, object],
    paper_id: str,
    source_name: str,
    default_name: str,
) -> dict[str, object] | None:
    entity_type = str(item.get("entity_type") or "material").strip().lower()
    if entity_type not in {"gene", "material", "molecule", "simulation", "protein", "structure"}:
        entity_type = "material"

    name = str(item.get("name") or default_name or "LLM entity").strip()
    description = str(item.get("description") or "LLM extracted entity").strip()
    if not name and not description:
        return None

    return {
        "entity_id": f"{paper_id}-llm-{name.lower().replace(' ', '-')[:24] or 'entity'}",
        "entity_type": entity_type,
        "name": name,
        "description": description,
        "temperature_max": _safe_float(item.get("temperature_max")),
        "strength": _safe_float(item.get("strength")),
        "conductivity": _safe_float(item.get("conductivity")),
        "ph": None,
        "salinity": None,
        "source": f"{source_name} LLM agent",
    }


def _safe_float(value: object) -> float | None:
    try:
        if value in (None, "", "NA", "null"):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def infer_primary_entity_type(text: str) -> str:
    if _contains_any(text, MATERIAL_KEYWORDS):
        return "material"
    if _contains_any(text, MOLECULE_KEYWORDS):
        return "molecule"
    if _contains_any(text, BIO_KEYWORDS):
        return "gene"
    if _contains_any(text, SIMULATION_KEYWORDS):
        return "simulation"
    return "material"


def _make_entity(
    paper_id: str,
    entity_type: str,
    name: str,
    description: str,
    source: str,
    combined_text: str,
) -> dict[str, object]:
    entity_name = name.strip() or "Unnamed literature entity"
    return {
        "entity_id": f"{paper_id}-{entity_type}-{slugify(entity_name)}",
        "entity_type": entity_type,
        "name": entity_name,
        "description": description.strip(),
        "temperature_max": extract_first_float(TEMPERATURE_PATTERN, combined_text),
        "strength": extract_first_float(STRENGTH_PATTERN, combined_text),
        "conductivity": extract_first_float(CONDUCTIVITY_PATTERN, combined_text),
        "ph": None,
        "salinity": None,
        "source": source,
    }


def extract_gene_name(text: str) -> str | None:
    for match in GENE_PATTERN.findall(text):
        if match in GENE_BLACKLIST:
            continue
        return match
    return None


def extract_material_name(text: str, title: str) -> str | None:
    formula = FORMULA_PATTERN.search(text)
    if formula:
        return formula.group(0)
    for keyword in MATERIAL_KEYWORDS:
        if keyword in title.lower():
            return keyword.title()
    return None


def extract_molecule_name(text: str, title: str) -> str | None:
    for keyword in MOLECULE_KEYWORDS:
        if keyword in title.lower():
            return keyword.title()
    formula = FORMULA_PATTERN.search(text)
    if formula:
        return formula.group(0)
    return None


def extract_first_float(pattern: re.Pattern[str], text: str) -> float | None:
    match = pattern.search(text)
    if not match:
        return None
    try:
        return float(match.group(1))
    except ValueError:
        return None


def _extract_text(node: ET.Element | None) -> str:
    if node is None:
        return ""
    return "".join(node.itertext()).strip()


def _contains_any(text: str, keywords: list[str]) -> bool:
    return any(keyword in text for keyword in keywords)


def _truncate_title(title: str, word_count: int) -> str:
    words = [word for word in title.split() if word]
    if not words:
        return "Untitled paper"
    return " ".join(words[:word_count])


def slugify(value: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9]+", "-", value.strip().lower())
    return slug.strip("-") or "entity"


def merge_entities_into_index(
    extracted_entities: pd.DataFrame, parquet_path: str | Path = PARQUET_PATH
) -> pd.DataFrame:
    destination = Path(parquet_path)
    if destination.exists():
        existing = pd.read_parquet(destination)
    else:
        existing = normalize_frame(pd.DataFrame())

    combined = concat_frames([existing, extracted_entities])
    combined = combined.drop_duplicates(subset=["entity_id"], keep="last").reset_index(drop=True)
    combined.to_parquet(destination, index=False)
    return combined


def refresh_vector_assets(parquet_path: str | Path = PARQUET_PATH) -> dict[str, object]:
    from universal_index.vector_search import (
        build_semantic_documents,
        get_cached_model,
        rebuild_collection,
        run_semantic_query,
        write_outputs,
    )

    documents = build_semantic_documents(pd.read_parquet(parquet_path))
    model = get_cached_model(EMBEDDING_MODEL_NAME)
    collection = rebuild_collection(
        documents=documents,
        model=model,
        chroma_dir=CHROMA_DIR,
        collection_name=CHROMA_COLLECTION_NAME,
        batch_size=128,
    )
    query_results = run_semantic_query(
        collection=collection,
        model=model,
        query_text="latest literature derived high temperature self healing design",
        candidate_pool=128,
        top_k=8,
    )
    write_outputs(
        documents=documents,
        query_results=query_results,
        query_text="latest literature derived high temperature self healing design",
        model_name=EMBEDDING_MODEL_NAME,
        chroma_dir=CHROMA_DIR,
        collection_name=CHROMA_COLLECTION_NAME,
    )
    return {
        "rows_indexed": int(len(documents)),
        "query_rows": int(len(query_results)),
    }


def write_literature_outputs(
    papers: pd.DataFrame,
    extracted_entities: pd.DataFrame,
    merged_index: pd.DataFrame,
    vector_refresh: dict[str, object] | None,
) -> dict[str, object]:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    papers.to_csv(LITERATURE_PAPERS_RAW_PATH, index=False)
    extracted_entities.to_parquet(LITERATURE_ENTITY_PATH, index=False)

    validation_results = run_duckdb_validation(PARQUET_PATH, DUCKDB_PATH)
    validation_results.to_csv(VALIDATION_CSV_PATH, index=False)

    build_summary = {}
    if BUILD_SUMMARY_PATH.exists():
        build_summary = json.loads(BUILD_SUMMARY_PATH.read_text(encoding="utf-8"))
    build_summary["rows_total"] = int(len(merged_index))
    build_summary["rows_by_type"] = (
        merged_index["entity_type"].value_counts(dropna=False).to_dict()
    )
    build_summary["sources"] = sorted(
        set(merged_index["source"].dropna().astype(str).tolist())
    )
    build_summary["validation_rows"] = int(len(validation_results))
    BUILD_SUMMARY_PATH.write_text(json.dumps(build_summary, indent=2), encoding="utf-8")

    summary = {
        "papers_fetched": int(len(papers)),
        "papers_by_source": papers["paper_source"].value_counts(dropna=False).to_dict(),
        "entities_extracted": int(len(extracted_entities)),
        "entities_by_type": extracted_entities["entity_type"]
        .value_counts(dropna=False)
        .to_dict(),
        "merged_rows_total": int(len(merged_index)),
        "validation_rows": int(len(validation_results)),
        "paper_csv_path": str(LITERATURE_PAPERS_RAW_PATH),
        "entity_parquet_path": str(LITERATURE_ENTITY_PATH),
        "vector_refresh": vector_refresh,
    }
    LITERATURE_SUMMARY_PATH.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    return summary


def main() -> None:
    args = parse_args()
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    requested_pubmed = max(int(args.pubmed), 0)
    requested_arxiv = max(int(args.arxiv), 0)

    try:
        pubmed_papers = fetch_pubmed_papers(
            sample_size=requested_pubmed,
            email=args.entrez_email,
            api_key=args.ncbi_api_key,
        )
    except Exception:
        pubmed_papers = generate_paper_fallback("PubMed", requested_pubmed)

    pubmed_papers = select_relevant_papers(pubmed_papers, requested_pubmed)
    if len(pubmed_papers) < requested_pubmed:
        missing = requested_pubmed - len(pubmed_papers)
        pubmed_papers = pd.concat(
            [pubmed_papers, generate_paper_fallback("PubMed", missing)],
            ignore_index=True,
        )

    try:
        arxiv_papers = fetch_arxiv_papers(sample_size=requested_arxiv)
    except Exception:
        arxiv_papers = generate_paper_fallback("arXiv", requested_arxiv)

    arxiv_papers = select_relevant_papers(arxiv_papers, requested_arxiv)
    if len(arxiv_papers) < requested_arxiv:
        missing = requested_arxiv - len(arxiv_papers)
        arxiv_papers = pd.concat(
            [arxiv_papers, generate_paper_fallback("arXiv", missing)],
            ignore_index=True,
        )

    papers = pd.concat([pubmed_papers, arxiv_papers], ignore_index=True)
    extracted_entities = extract_entities_from_papers(papers)
    merged_index = merge_entities_into_index(extracted_entities, parquet_path=PARQUET_PATH)

    vector_refresh = None
    if not args.skip_vector_refresh:
        vector_refresh = refresh_vector_assets(parquet_path=PARQUET_PATH)

    summary = write_literature_outputs(
        papers=papers,
        extracted_entities=extracted_entities,
        merged_index=merged_index,
        vector_refresh=vector_refresh,
    )
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()

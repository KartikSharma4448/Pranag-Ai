# Copyright (c) Kartik Sharma. GitHub: kartiksharma4448
"""
Real-time RSS feed ingestion for papers and entities.

Fetches latest publications from arXiv, PubMed, bioRxiv, chemRxiv, etc.
Supports differential ingestion: only processes papers newer than last fetch timestamp.

Target: 10,000 papers/month = ~330 papers/day across all feeds.
"""
from __future__ import annotations

import datetime
import json
from pathlib import Path
from typing import Any

import pandas as pd
import requests

from universal_index.config import PROCESSED_DIR, RAW_DIR, RANDOM_SEED

# RSS/API endpoints for scientific papers
ARXIV_API_URL = "https://export.arxiv.org/api/query"
PUBMED_ESEARCH_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
PUBMED_ESUMMARY_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi"
BIORXIV_API_URL = "https://api.biorxiv.org/jatsxml"  # bioRxiv API (experimental)
CROSSREF_API_URL = "https://api.crossref.org/works"

REQUEST_TIMEOUT_SECONDS = 30
USER_AGENT = "universal-index-feeds/0.1"

# Differential ingestion state file
FEED_STATE_PATH = PROCESSED_DIR / "feed_state.json"


def load_feed_state() -> dict[str, object]:
    """Load last fetch timestamp for each feed."""
    if FEED_STATE_PATH.exists():
        try:
            with open(FEED_STATE_PATH) as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            pass
    
    # Initialize with default state
    return {
        "arxiv_last_fetch_utc": None,
        "pubmed_last_fetch_utc": None,
        "crossref_last_fetch_utc": None,
        "seen_paper_ids": {},
        "total_papers_ingested": 0,
    }


def save_feed_state(state: dict[str, object]) -> None:
    """Save last fetch timestamp for each feed."""
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    with open(FEED_STATE_PATH, "w") as f:
        json.dump(state, f, indent=2, default=str)


def _filter_new_papers(
    source_name: str,
    frame: pd.DataFrame,
    state: dict[str, object],
) -> pd.DataFrame:
    if frame.empty:
        return frame

    seen = state.get("seen_paper_ids")
    if not isinstance(seen, dict):
        seen = {}

    source_seen = seen.get(source_name)
    if not isinstance(source_seen, list):
        source_seen = []

    existing = {str(item) for item in source_seen}
    deduped = frame[~frame["paper_id"].astype(str).isin(existing)].copy()

    merged = list(dict.fromkeys(source_seen + deduped["paper_id"].astype(str).tolist()))
    seen[source_name] = merged[-50000:]
    state["seen_paper_ids"] = seen

    return deduped.reset_index(drop=True)


def fetch_arxiv_papers(
    query: str = "cat:materials.mtrl-th OR cat:physics.chem-ph",
    max_results: int = 100,
    days_back: int = 7,
) -> pd.DataFrame:
    """
    Fetch recent papers from arXiv using REST API.
    
    Args:
        query: arXiv search query (default: materials + chemistry)
        max_results: Max papers to return per call
        days_back: Only fetch papers from past N days
    
    Returns:
        DataFrame with paper metadata: arxiv_id, title, authors, published, abstract, url
    """
    state = load_feed_state()
    last_fetch = state.get("arxiv_last_fetch_utc")
    
    # Build search query with date filter
    if last_fetch:
        # Only fetch papers newer than last fetch
        query_with_date = f"({query}) AND submittedDate:[{last_fetch} TO 2500-01-01T23:59:59Z]"
    else:
        # First run: fetch from N days ago
        cutoff_date = (
            datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=days_back)
        ).isoformat()
        query_with_date = f"({query}) AND submittedDate:[{cutoff_date} TO 2500-01-01T23:59:59Z]"
    
    params = {
        "search_query": query_with_date,
        "start": 0,
        "max_results": min(max_results, 200),  # arXiv max is 200
        "sortBy": "submittedDate",
        "sortOrder": "descending",
    }
    
    session = requests.Session()
    session.trust_env = False
    session.headers.update({"User-Agent": USER_AGENT})
    
    try:
        response = session.get(ARXIV_API_URL, params=params, timeout=REQUEST_TIMEOUT_SECONDS)
        response.raise_for_status()
    except requests.RequestException as error:
        print(f"⚠️  arXiv fetch failed: {error}")
        return pd.DataFrame()
    
    # Parse Atom XML response
    try:
        import xml.etree.ElementTree as ET
        root = ET.fromstring(response.content)
        
        records = []
        for entry in root.findall("{http://www.w3.org/2005/Atom}entry"):
            arxiv_id = entry.find("{http://www.w3.org/2005/Atom}id").text.split("/abs/")[-1]
            title = entry.find("{http://www.w3.org/2005/Atom}title").text.strip()
            published = entry.find("{http://www.w3.org/2005/Atom}published").text
            summary = entry.find("{http://www.w3.org/2005/Atom}summary").text.strip()
            
            authors = []
            for author_elem in entry.findall("{http://www.w3.org/2005/Atom}author"):
                name_elem = author_elem.find("{http://www.w3.org/2005/Atom}name")
                if name_elem is not None:
                    authors.append(name_elem.text)
            
            records.append({
                "paper_id": f"arxiv-{arxiv_id}",
                "source": "arXiv",
                "title": title,
                "authors": " | ".join(authors[:5]),  # First 5 authors
                "published": published,
                "abstract": summary,
                "url": f"https://arxiv.org/abs/{arxiv_id}",
            })
        
        deduped = _filter_new_papers("arxiv", pd.DataFrame(records), state)

        # Update last fetch timestamp
        state["arxiv_last_fetch_utc"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
        state["total_papers_ingested"] = state.get("total_papers_ingested", 0) + len(deduped)
        save_feed_state(state)
        
        print(f"✅ arXiv: fetched {len(deduped)} new papers")
        return deduped
    except Exception as error:
        print(f"⚠️  arXiv parse failed: {error}")
        return pd.DataFrame()


def fetch_pubmed_papers(
    query: str = "materials OR chemistry OR proteins",
    max_results: int = 100,
    email: str = "bot@example.com",
    api_key: str | None = None,
) -> pd.DataFrame:
    """
    Fetch recent papers from PubMed using E-utilities API.
    
    Args:
        query: PubMed search query
        max_results: Max papers to return
        email: Required for PubMed (identify yourself)
        api_key: Optional API key (increases rate limits)
    
    Returns:
        DataFrame with paper metadata
    """
    state = load_feed_state()
    last_fetch = state.get("pubmed_last_fetch_utc")
    
    session = requests.Session()
    session.trust_env = False
    session.headers.update({"User-Agent": USER_AGENT})
    
    try:
        # Search for papers
        search_params = {
            "db": "pubmed",
            "term": query,
            "retmax": min(max_results, 10000),
            "retmode": "json",
            "email": email,
            "sort": "pub_date",
        }
        if last_fetch:
            try:
                parsed = datetime.datetime.fromisoformat(str(last_fetch).replace("Z", "+00:00"))
                search_params["datetype"] = "pdat"
                search_params["mindate"] = parsed.strftime("%Y/%m/%d")
            except Exception:
                pass
        if api_key:
            search_params["api_key"] = api_key
        
        search_response = session.get(PUBMED_ESEARCH_URL, params=search_params, timeout=REQUEST_TIMEOUT_SECONDS)
        search_response.raise_for_status()
        search_data = search_response.json()
        
        pubmed_ids = search_data.get("esearchresult", {}).get("idlist", [])[:max_results]
        if not pubmed_ids:
            print("⚠️  PubMed: no results found")
            return pd.DataFrame()
        
        # Fetch paper details
        summary_params = {
            "db": "pubmed",
            "id": ",".join(pubmed_ids),
            "retmode": "json",
            "email": email,
        }
        if api_key:
            summary_params["api_key"] = api_key
        
        fetch_response = session.get(PUBMED_ESUMMARY_URL, params=summary_params, timeout=REQUEST_TIMEOUT_SECONDS)
        fetch_response.raise_for_status()
        fetch_data = fetch_response.json()
        
        records = []
        for doc in fetch_data.get("result", {}).get("uids", []):
            article = fetch_data.get("result", {}).get(doc, {})
            if "error" in article:
                continue

            author_items = article.get("authors", [])
            author_names = " | ".join(
                str(author.get("name", "")).strip()
                for author in author_items[:5]
                if str(author.get("name", "")).strip()
            )
            
            records.append({
                "paper_id": f"pubmed-{doc}",
                "source": "PubMed",
                "title": article.get("title", "Unknown"),
                "authors": author_names,
                "published": article.get("pubdate", "Unknown"),
                "abstract": article.get("elocationid", ""),
                "url": f"https://pubmed.ncbi.nlm.nih.gov/{doc}/",
            })
        
        deduped = _filter_new_papers("pubmed", pd.DataFrame(records), state)

        # Update state
        state["pubmed_last_fetch_utc"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
        state["total_papers_ingested"] = state.get("total_papers_ingested", 0) + len(deduped)
        save_feed_state(state)
        
        print(f"✅ PubMed: fetched {len(deduped)} new papers")
        return deduped
    except Exception as error:
        print(f"⚠️  PubMed fetch failed: {error}")
        return pd.DataFrame()


def fetch_crossref_papers(
    query: str = "materials science OR chemistry",
    max_results: int = 100,
) -> pd.DataFrame:
    """
    Fetch recent papers from Crossref API (open access, free).
    
    Args:
        query: Crossref search query
        max_results: Max papers to return
    
    Returns:
        DataFrame with paper metadata
    """
    state = load_feed_state()
    last_fetch = state.get("crossref_last_fetch_utc")
    
    session = requests.Session()
    session.trust_env = False
    session.headers.update({"User-Agent": USER_AGENT})
    
    try:
        params = {
            "query": query,
            "rows": min(max_results, 1000),
            "sort": "published",
            "order": "desc",
            "filter": "is-open-access:true",
        }
        if last_fetch:
            try:
                parsed = datetime.datetime.fromisoformat(str(last_fetch).replace("Z", "+00:00"))
                params["filter"] = (
                    f"is-open-access:true,from-pub-date:{parsed.strftime('%Y-%m-%d')}"
                )
            except Exception:
                pass
        
        response = session.get(CROSSREF_API_URL, params=params, timeout=REQUEST_TIMEOUT_SECONDS)
        response.raise_for_status()
        data = response.json()
        
        records = []
        for item in data.get("message", {}).get("items", [])[:max_results]:
            authors = item.get("author", [])
            author_names = " | ".join([f"{a.get('given', '')} {a.get('family', '')}".strip() for a in authors[:5]])
            
            records.append({
                "paper_id": f"crossref-{item.get('DOI', 'unknown')}",
                "source": "Crossref",
                "title": item.get("title", ["Unknown"])[0] if item.get("title") else "Unknown",
                "authors": author_names,
                "published": item.get("published-print", {}).get("date-parts", [[""] * 3])[0],
                "abstract": item.get("abstract", ""),
                "url": f"https://doi.org/{item.get('DOI', '')}",
            })
        
        deduped = _filter_new_papers("crossref", pd.DataFrame(records), state)

        # Update state
        state["crossref_last_fetch_utc"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
        state["total_papers_ingested"] = state.get("total_papers_ingested", 0) + len(deduped)
        save_feed_state(state)
        
        print(f"✅ Crossref: fetched {len(deduped)} new papers")
        return deduped
    except Exception as error:
        print(f"⚠️  Crossref fetch failed: {error}")
        return pd.DataFrame()


def fetch_all_feeds(include_arxiv: bool = True, include_pubmed: bool = True, include_crossref: bool = True) -> pd.DataFrame:
    """
    Fetch from all enabled feeds and deduplicate.
    
    Returns:
        Combined DataFrame from all feeds
    """
    frames = []
    
    if include_arxiv:
        frames.append(fetch_arxiv_papers())
    
    if include_pubmed:
        frames.append(fetch_pubmed_papers())
    
    if include_crossref:
        frames.append(fetch_crossref_papers())
    
    non_empty_frames = [f for f in frames if not f.empty]
    if not non_empty_frames:
        return pd.DataFrame(
            columns=["paper_id", "source", "title", "authors", "published", "abstract", "url"]
        )

    combined = pd.concat(non_empty_frames, ignore_index=True)
    
    # Deduplicate by title (simple heuristic)
    if not combined.empty:
        combined = combined.drop_duplicates(subset=["title"], keep="first").reset_index(drop=True)
    
    return combined

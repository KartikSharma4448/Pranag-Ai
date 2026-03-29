# Copyright (c) Kartik Sharma. GitHub: kartiksharma4448
from __future__ import annotations

import argparse
import json
import math
from functools import lru_cache
from pathlib import Path

import chromadb
import pandas as pd
from sentence_transformers import SentenceTransformer

from universal_index.config import (
    CHROMA_COLLECTION_NAME,
    CHROMA_DIR,
    EMBEDDING_MODEL_NAME,
    PARQUET_PATH,
    PROCESSED_DIR,
    VECTOR_DOCUMENTS_PATH,
    VECTOR_QUERY_RESULTS_PATH,
    VECTOR_SUMMARY_PATH,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build the Day 2 Chroma vector index.")
    parser.add_argument("--parquet-path", default=str(PARQUET_PATH))
    parser.add_argument("--query", default="self healing high temperature material")
    parser.add_argument("--top-k", type=int, default=9)
    parser.add_argument("--candidate-pool", type=int, default=128)
    parser.add_argument("--model-name", default=EMBEDDING_MODEL_NAME)
    parser.add_argument("--collection-name", default=CHROMA_COLLECTION_NAME)
    parser.add_argument("--chroma-dir", default=str(CHROMA_DIR))
    parser.add_argument("--batch-size", type=int, default=128)
    return parser.parse_args()


def load_universal_index(parquet_path: str | Path) -> pd.DataFrame:
    path = Path(parquet_path)
    if not path.exists():
        raise FileNotFoundError(
            f"Universal index not found at {path}. Run `python -m universal_index.build` first."
        )
    frame = pd.read_parquet(path)
    if frame.empty:
        raise RuntimeError("Universal index is empty.")
    return frame.fillna(value=pd.NA)


def build_semantic_documents(frame: pd.DataFrame) -> pd.DataFrame:
    documents = frame.copy()
    documents["semantic_text"] = documents.apply(build_semantic_text, axis=1)
    return documents


def build_semantic_text(row: pd.Series) -> str:
    entity_type = _clean_text(row.get("entity_type")) or "entity"
    name = _clean_text(row.get("name")) or "unnamed entity"
    description = _clean_text(row.get("description"))
    source = _clean_text(row.get("source"))

    pieces = [f"{entity_type.title()} {name}."]

    if description:
        pieces.append(description)

    measurement_sentence = _build_measurement_sentence(row)
    if measurement_sentence:
        pieces.append(measurement_sentence)

    pieces.append(_build_cross_domain_hint(row))

    if source:
        pieces.append(f"Source: {source}.")

    return " ".join(piece for piece in pieces if piece).strip()


def _build_measurement_sentence(row: pd.Series) -> str:
    measurements: list[str] = []
    mapping = {
        "temperature_max": "temperature max",
        "strength": "strength",
        "conductivity": "conductivity",
        "ph": "pH",
        "salinity": "salinity",
    }
    for column, label in mapping.items():
        value = row.get(column)
        if _is_missing(value):
            continue
        if isinstance(value, float):
            measurements.append(f"{label} {value:.2f}")
        else:
            measurements.append(f"{label} {value}")

    if not measurements:
        return ""

    return "Measured properties: " + ", ".join(measurements) + "."


def _build_cross_domain_hint(row: pd.Series) -> str:
    entity_type = _clean_text(row.get("entity_type"))
    description = _clean_text(row.get("description")).lower()
    name = _clean_text(row.get("name")).lower()
    combined = f"{name} {description}"

    if entity_type == "gene":
        hints = [
            "Cross-domain hint: biological adaptation and repair signal relevant to bio-inspired materials and resilient system design.",
            "Relevant concepts: stress response, cellular recovery, biomineralization, and healing analogies in engineering.",
        ]
        if any(keyword in combined for keyword in ["heat", "shock", "stress"]):
            hints.append("Additional signal: heat tolerance and thermal adaptation cues.")
        if any(keyword in combined for keyword in ["immune", "globulin", "repair", "kinase"]):
            hints.append("Additional signal: recovery, defense, and repair pathways.")
        return " ".join(hints)

    if entity_type == "material":
        hints = [
            "Cross-domain hint: advanced material candidate for self-healing composites, high-temperature service, thermal resistance, and structural resilience.",
            "Relevant concepts: coatings, ceramics, alloys, conductivity screening, and heat-facing materials engineering.",
        ]
        if any(keyword in combined for keyword in ["o", "si", "al", "ti", "b", "c", "n"]):
            hints.append("Additional signal: ceramic-like or semiconductor-like behavior for self-healing coatings and heat-facing systems.")
        return " ".join(hints)

    if entity_type == "molecule":
        hints = [
            "Cross-domain hint: molecular building block for polymer formulation, coatings, self-healing materials, and interface chemistry.",
            "Relevant concepts: reactive chemistry, cross-linking, surface modification, and materials formulation.",
        ]
        if any(
            keyword in combined
            for keyword in ["amine", "hydroxy", "carbox", "epoxy", "oxide", "acetyl"]
        ):
            hints.append("Additional signal: functional groups compatible with network formation and self-healing polymer design.")
        return " ".join(hints)

    if entity_type == "soil":
        return (
            "Cross-domain hint: environmental matrix connecting mineral behavior, salinity, pH, conductivity, and temperature exposure. "
            "Relevant concepts: substrate stability, corrosion context, and deployment environment for high-temperature systems."
        )

    if entity_type == "simulation":
        return (
            "Cross-domain hint: physics and engineering surrogate for heat transfer, structural loading, thermal expansion, and materials screening. "
            "Relevant concepts: CFD, FEA, vacuum exposure, radiation-facing systems, and pre-calculated performance envelopes."
        )

    return "Cross-domain hint: searchable scientific entity for biology, chemistry, materials, and environment reasoning."


def _clean_text(value: object) -> str:
    if _is_missing(value):
        return ""
    return str(value).strip()


def _is_missing(value: object) -> bool:
    return value is None or (isinstance(value, float) and math.isnan(value)) or pd.isna(value)


def initialize_model(model_name: str) -> SentenceTransformer:
    return SentenceTransformer(model_name)


@lru_cache(maxsize=2)
def get_cached_model(model_name: str) -> SentenceTransformer:
    return initialize_model(model_name)


def rebuild_collection(
    documents: pd.DataFrame,
    model: SentenceTransformer,
    chroma_dir: str | Path,
    collection_name: str,
    batch_size: int,
) -> chromadb.Collection:
    persist_path = Path(chroma_dir)
    persist_path.mkdir(parents=True, exist_ok=True)

    client = chromadb.PersistentClient(path=str(persist_path))
    try:
        client.delete_collection(collection_name)
    except Exception:
        pass

    collection = client.create_collection(
        name=collection_name,
        metadata={"hnsw:space": "cosine"},
    )

    texts = documents["semantic_text"].astype(str).tolist()
    embeddings = model.encode(
        texts,
        batch_size=batch_size,
        normalize_embeddings=True,
        show_progress_bar=True,
    )

    ids = documents["entity_id"].astype(str).tolist()
    metadatas = [build_metadata(row) for row in documents.to_dict(orient="records")]

    for start in range(0, len(documents), batch_size):
        end = start + batch_size
        collection.add(
            ids=ids[start:end],
            documents=texts[start:end],
            embeddings=embeddings[start:end].tolist(),
            metadatas=metadatas[start:end],
        )

    return collection


def load_collection(chroma_dir: str | Path, collection_name: str) -> chromadb.Collection:
    client = chromadb.PersistentClient(path=str(chroma_dir))
    return client.get_collection(collection_name)


def ensure_vector_collection(
    parquet_path: str | Path,
    model_name: str,
    chroma_dir: str | Path,
    collection_name: str,
    batch_size: int,
) -> chromadb.Collection:
    try:
        return load_collection(chroma_dir=chroma_dir, collection_name=collection_name)
    except Exception:
        model = get_cached_model(model_name)
        documents = build_semantic_documents(load_universal_index(parquet_path))
        return rebuild_collection(
            documents=documents,
            model=model,
            chroma_dir=chroma_dir,
            collection_name=collection_name,
            batch_size=batch_size,
        )


def build_metadata(row: dict[str, object]) -> dict[str, object]:
    metadata = {
        "entity_id": str(row.get("entity_id") or ""),
        "entity_type": str(row.get("entity_type") or ""),
        "name": str(row.get("name") or ""),
        "source": str(row.get("source") or ""),
    }

    for column in ["temperature_max", "strength", "conductivity", "ph", "salinity"]:
        value = row.get(column)
        if _is_missing(value):
            continue
        metadata[column] = float(value)

    return metadata


def run_semantic_query(
    collection: chromadb.Collection,
    model: SentenceTransformer,
    query_text: str,
    candidate_pool: int,
    top_k: int,
) -> pd.DataFrame:
    base_results = query_collection(
        collection=collection,
        model=model,
        query_text=query_text,
        n_results=max(candidate_pool, top_k),
        retrieval_route="base",
    )
    if base_results.empty:
        return base_results

    anchor_frames: list[pd.DataFrame] = []
    entity_types = ["material", "molecule", "gene", "soil"]
    if top_k >= 5:
        entity_types.append("simulation")

    for entity_type in entity_types:
        expanded_query = expand_query_for_entity_type(query_text, entity_type)
        expanded_results = query_collection(
            collection=collection,
            model=model,
            query_text=expanded_query,
            n_results=3,
            retrieval_route=f"{entity_type}_expansion",
            where={"entity_type": entity_type},
        )
        if expanded_results.empty:
            continue
        matched = expanded_results.head(1)
        if not matched.empty:
            anchor_frames.append(matched)

    anchor_results = (
        pd.concat(anchor_frames, ignore_index=True) if anchor_frames else pd.DataFrame()
    )
    return select_cross_domain_results(
        base_results=base_results,
        anchor_results=anchor_results,
        top_k=top_k,
    )


def semantic_search(
    query_text: str,
    top_k: int,
    candidate_pool: int,
    model_name: str = EMBEDDING_MODEL_NAME,
    chroma_dir: str | Path = CHROMA_DIR,
    collection_name: str = CHROMA_COLLECTION_NAME,
    parquet_path: str | Path = PARQUET_PATH,
    batch_size: int = 128,
) -> pd.DataFrame:
    model = get_cached_model(model_name)
    collection = ensure_vector_collection(
        parquet_path=parquet_path,
        model_name=model_name,
        chroma_dir=chroma_dir,
        collection_name=collection_name,
        batch_size=batch_size,
    )
    return run_semantic_query(
        collection=collection,
        model=model,
        query_text=query_text,
        candidate_pool=candidate_pool,
        top_k=top_k,
    )


def query_collection(
    collection: chromadb.Collection,
    model: SentenceTransformer,
    query_text: str,
    n_results: int,
    retrieval_route: str,
    where: dict[str, object] | None = None,
) -> pd.DataFrame:
    query_embedding = model.encode(
        [query_text],
        normalize_embeddings=True,
        show_progress_bar=False,
    )[0].tolist()

    raw = collection.query(
        query_embeddings=[query_embedding],
        n_results=n_results,
        include=["documents", "metadatas", "distances"],
        where=where,
    )

    rows: list[dict[str, object]] = []
    metadatas = raw.get("metadatas", [[]])[0]
    documents = raw.get("documents", [[]])[0]
    distances = raw.get("distances", [[]])[0]

    for rank, (metadata, document, distance) in enumerate(
        zip(metadatas, documents, distances), start=1
    ):
        metadata = metadata or {}
        rows.append(
            {
                "rank": rank,
                "entity_id": metadata.get("entity_id"),
                "entity_type": metadata.get("entity_type"),
                "name": metadata.get("name"),
                "source": metadata.get("source"),
                "temperature_max": metadata.get("temperature_max"),
                "strength": metadata.get("strength"),
                "conductivity": metadata.get("conductivity"),
                "ph": metadata.get("ph"),
                "salinity": metadata.get("salinity"),
                "distance": distance,
                "similarity_estimate": None if distance is None else 1 - float(distance),
                "semantic_text": document,
                "query_variant": query_text,
                "retrieval_route": retrieval_route,
            }
        )

    return pd.DataFrame(rows)


def expand_query_for_entity_type(query_text: str, entity_type: str) -> str:
    if entity_type == "material":
        return (
            f"{query_text}. high temperature structural material, self-healing composite, "
            "thermal resistance, coating, ceramic, conductive material"
        )
    if entity_type == "molecule":
        return (
            f"{query_text}. self-healing polymer chemistry, reactive molecule, coating additive, "
            "cross-linking chemistry, formulation molecule"
        )
    if entity_type == "gene":
        return (
            f"{query_text}. repair gene, stress response, biological adaptation, "
            "bio-inspired material design, thermal resilience"
        )
    if entity_type == "soil":
        return (
            f"{query_text}. soil environment, mineral substrate, salinity, pH, heat exposure, "
            "deployment environment"
        )
    if entity_type == "simulation":
        return (
            f"{query_text}. CFD template, thermal expansion, engineering simulation, "
            "heat transfer model, structural heat-load benchmark"
        )
    return query_text


def select_cross_domain_results(
    base_results: pd.DataFrame,
    anchor_results: pd.DataFrame,
    top_k: int,
) -> pd.DataFrame:
    ordered = base_results.sort_values(by=["distance", "rank"], ascending=[True, True]).reset_index(
        drop=True
    )

    selected_rows: list[pd.Series] = []
    used_ids: set[str] = set()

    if not anchor_results.empty:
        anchor_ordered = anchor_results.drop_duplicates(subset=["entity_id"]).reset_index(drop=True)
    else:
        anchor_ordered = pd.DataFrame()

    entity_types = ["material", "molecule", "gene", "soil"]
    if top_k >= 5:
        entity_types.append("simulation")

    for entity_type in entity_types:
        if len(selected_rows) >= top_k:
            break
        matches = anchor_ordered[anchor_ordered["entity_type"] == entity_type]
        if matches.empty:
            continue
        row = matches.iloc[0]
        entity_id = str(row["entity_id"])
        if entity_id in used_ids:
            continue
        selected_rows.append(row)
        used_ids.add(entity_id)

    for _, row in ordered.iterrows():
        if len(selected_rows) >= top_k:
            break
        entity_id = str(row["entity_id"])
        if entity_id in used_ids:
            continue
        selected_rows.append(row)
        used_ids.add(entity_id)

    final_frame = pd.DataFrame(selected_rows).reset_index(drop=True)
    final_frame.insert(0, "final_rank", range(1, len(final_frame) + 1))
    return final_frame


def write_outputs(
    documents: pd.DataFrame,
    query_results: pd.DataFrame,
    query_text: str,
    model_name: str,
    chroma_dir: str | Path,
    collection_name: str,
) -> None:
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    documents.to_parquet(VECTOR_DOCUMENTS_PATH, index=False)

    result_payload = {
        "query": query_text,
        "model_name": model_name,
        "collection_name": collection_name,
        "results": query_results.where(query_results.notna(), None).to_dict(orient="records"),
    }
    VECTOR_QUERY_RESULTS_PATH.write_text(json.dumps(result_payload, indent=2), encoding="utf-8")

    summary = {
        "rows_indexed": int(len(documents)),
        "collection_name": collection_name,
        "chroma_dir": str(chroma_dir),
        "model_name": model_name,
        "query": query_text,
        "result_entity_types": query_results["entity_type"].tolist() if not query_results.empty else [],
        "vector_documents_path": str(VECTOR_DOCUMENTS_PATH),
        "query_results_path": str(VECTOR_QUERY_RESULTS_PATH),
    }
    VECTOR_SUMMARY_PATH.write_text(json.dumps(summary, indent=2), encoding="utf-8")


def main() -> None:
    args = parse_args()
    documents = build_semantic_documents(load_universal_index(args.parquet_path))
    model = get_cached_model(args.model_name)
    collection = rebuild_collection(
        documents=documents,
        model=model,
        chroma_dir=args.chroma_dir,
        collection_name=args.collection_name,
        batch_size=args.batch_size,
    )
    query_results = run_semantic_query(
        collection=collection,
        model=model,
        query_text=args.query,
        candidate_pool=args.candidate_pool,
        top_k=args.top_k,
    )
    write_outputs(
        documents=documents,
        query_results=query_results,
        query_text=args.query,
        model_name=args.model_name,
        chroma_dir=args.chroma_dir,
        collection_name=args.collection_name,
    )
    print(
        json.dumps(
            {
                "rows_indexed": int(len(documents)),
                "query": args.query,
                "top_k": int(len(query_results)),
                "result_entity_types": query_results["entity_type"].tolist()
                if not query_results.empty
                else [],
                "query_results_path": str(VECTOR_QUERY_RESULTS_PATH),
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()

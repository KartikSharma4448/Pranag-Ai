# Copyright (c) Kartik Sharma. GitHub: kartiksharma4448
from __future__ import annotations

import argparse
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from universal_index.build import run_duckdb_validation
from universal_index.cache import build_cache_backend
from universal_index.config import (
    BUILD_SUMMARY_PATH,
    DATA_DIR,
    DEFAULT_COUNTS,
    DUCKDB_PATH,
    ENTREZ_EMAIL,
    INGESTION_MAX_WORKERS,
    INGESTION_SUMMARY_PATH,
    LAKE_DIR,
    LITERATURE_ENTITY_PATH,
    LITERATURE_PAPERS_RAW_PATH,
    MP_API_KEY,
    NCBI_API_KEY,
    PARQUET_PATH,
    PROCESSED_DIR,
    RAW_DIR,
    REDIS_STREAM_KEY,
    RANDOM_SEED,
    VALIDATION_CSV_PATH,
)
from universal_index.literature_agent import (
    extract_entities_from_papers,
    fetch_arxiv_papers,
    fetch_crossref_journal_papers,
    fetch_pubmed_papers,
    generate_paper_fallback,
    refresh_vector_assets,
    select_relevant_papers,
)
from universal_index.schema import concat_frames, normalize_frame
from universal_index.sources import (
    fetch_alphafold_structures,
    fetch_boltz1_structures,
    fetch_chembl_bioactivity,
    fetch_genbank_fallback,
    fetch_genbank_metadata,
    fetch_gene_fallback,
    fetch_genes,
    fetch_materials,
    fetch_pubchem_fallback,
    fetch_pubchem_molecules,
    fetch_pdb_fallback,
    fetch_pdb_structures,
    fetch_zinc20_metadata,
    fetch_uniprot_fallback,
    fetch_uniprot_proteins,
    generate_aflow_materials,
    generate_nasa_material_records,
    generate_nist_thermo_records,
    generate_oqmd_materials,
    generate_openfoam_records,
    generate_simulation_records,
    generate_soil_records,
)
from universal_index.state import PipelineStateStore
from universal_index.storage import build_object_storage_client


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run parallel ingestion and materialize lake partitions.")
    parser.add_argument("--genes", type=int, default=DEFAULT_COUNTS["genes"])
    parser.add_argument("--genbank", type=int, default=DEFAULT_COUNTS["genbank"])
    parser.add_argument("--materials", type=int, default=DEFAULT_COUNTS["materials"])
    parser.add_argument("--molecules", type=int, default=DEFAULT_COUNTS["molecules"])
    parser.add_argument("--proteins", type=int, default=DEFAULT_COUNTS["proteins"])
    parser.add_argument("--structures", type=int, default=DEFAULT_COUNTS["structures"])
    parser.add_argument("--chembl", type=int, default=DEFAULT_COUNTS["chembl"])
    parser.add_argument("--aflow", type=int, default=DEFAULT_COUNTS["aflow"])
    parser.add_argument("--oqmd", type=int, default=DEFAULT_COUNTS["oqmd"])
    parser.add_argument("--alphafold", type=int, default=DEFAULT_COUNTS["alphafold"])
    parser.add_argument("--boltz1", type=int, default=DEFAULT_COUNTS["boltz1"])
    parser.add_argument("--zinc20", type=int, default=DEFAULT_COUNTS["zinc20"])
    parser.add_argument("--nasa", type=int, default=DEFAULT_COUNTS["nasa"])
    parser.add_argument("--nist", type=int, default=DEFAULT_COUNTS["nist"])
    parser.add_argument("--openfoam", type=int, default=DEFAULT_COUNTS["openfoam"])
    parser.add_argument("--soil", type=int, default=DEFAULT_COUNTS["soil"])
    parser.add_argument("--simulations", type=int, default=DEFAULT_COUNTS["simulations"])
    parser.add_argument("--pubmed", type=int, default=4)
    parser.add_argument("--arxiv", type=int, default=4)
    parser.add_argument("--nature", type=int, default=2)
    parser.add_argument("--materials-today", type=int, default=2)
    parser.add_argument("--include-literature", action="store_true")
    parser.add_argument("--refresh-vectors", action="store_true")
    parser.add_argument("--max-workers", type=int, default=INGESTION_MAX_WORKERS)
    parser.add_argument("--entrez-email", default=ENTREZ_EMAIL)
    parser.add_argument("--ncbi-api-key", default=NCBI_API_KEY)
    parser.add_argument("--mp-api-key", default=MP_API_KEY)
    parser.add_argument("--seed", type=int, default=RANDOM_SEED)
    return parser.parse_args()


def ensure_directories() -> None:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    LAKE_DIR.mkdir(parents=True, exist_ok=True)


def load_genes(args: argparse.Namespace) -> pd.DataFrame:
    try:
        return fetch_genes(args.genes, email=args.entrez_email, api_key=args.ncbi_api_key)
    except Exception:
        return fetch_gene_fallback(args.genes, seed=args.seed)


def load_genbank(args: argparse.Namespace) -> pd.DataFrame:
    try:
        return fetch_genbank_metadata(args.genbank, email=args.entrez_email, api_key=args.ncbi_api_key)
    except Exception:
        return fetch_genbank_fallback(args.genbank)


def load_materials(args: argparse.Namespace) -> pd.DataFrame:
    return fetch_materials(args.materials, api_key=args.mp_api_key, seed=args.seed)


def load_aflow_materials(args: argparse.Namespace) -> pd.DataFrame:
    return generate_aflow_materials(args.aflow, seed=args.seed)


def load_oqmd_materials(args: argparse.Namespace) -> pd.DataFrame:
    return generate_oqmd_materials(args.oqmd, seed=args.seed)


def load_nasa_materials(args: argparse.Namespace) -> pd.DataFrame:
    return generate_nasa_material_records(args.nasa, seed=args.seed)


def load_aflow_materials(args: argparse.Namespace) -> pd.DataFrame:
    return generate_aflow_materials(args.aflow, seed=args.seed)


def load_oqmd_materials(args: argparse.Namespace) -> pd.DataFrame:
    return generate_oqmd_materials(args.oqmd, seed=args.seed)


def load_molecules(args: argparse.Namespace) -> pd.DataFrame:
    try:
        return fetch_pubchem_molecules(args.molecules)
    except Exception:
        return fetch_pubchem_fallback(args.molecules, seed=args.seed)


def load_zinc20(args: argparse.Namespace) -> pd.DataFrame:
    return fetch_zinc20_metadata(args.zinc20, seed=args.seed)


def load_soil(args: argparse.Namespace) -> pd.DataFrame:
    return generate_soil_records(args.soil, seed=args.seed)


def load_nist(args: argparse.Namespace) -> pd.DataFrame:
    return generate_nist_thermo_records(args.nist, seed=args.seed)


def load_openfoam(args: argparse.Namespace) -> pd.DataFrame:
    return generate_openfoam_records(args.openfoam, seed=args.seed)


def load_proteins(args: argparse.Namespace) -> pd.DataFrame:
    try:
        return fetch_uniprot_proteins(args.proteins, seed=args.seed)
    except Exception:
        return fetch_uniprot_fallback(args.proteins, seed=args.seed)


def load_alphafold(args: argparse.Namespace) -> pd.DataFrame:
    return fetch_alphafold_structures(args.alphafold, seed=args.seed)


def load_boltz1(args: argparse.Namespace) -> pd.DataFrame:
    return fetch_boltz1_structures(args.boltz1, seed=args.seed)


def load_structures(args: argparse.Namespace) -> pd.DataFrame:
    try:
        return fetch_pdb_structures(args.structures, seed=args.seed)
    except Exception:
        return fetch_pdb_fallback(args.structures, seed=args.seed)


def load_chembl(args: argparse.Namespace) -> pd.DataFrame:
    return fetch_chembl_bioactivity(args.chembl)


def load_simulations(args: argparse.Namespace) -> pd.DataFrame:
    return generate_simulation_records(args.simulations, seed=args.seed)


def load_literature(args: argparse.Namespace) -> tuple[pd.DataFrame, pd.DataFrame]:
    try:
        pubmed = fetch_pubmed_papers(args.pubmed, email=args.entrez_email, api_key=args.ncbi_api_key)
    except Exception:
        pubmed = generate_paper_fallback("PubMed", args.pubmed)
    pubmed = select_relevant_papers(pubmed, args.pubmed)
    if len(pubmed) < args.pubmed:
        pubmed = pd.concat(
            [pubmed, generate_paper_fallback("PubMed", args.pubmed - len(pubmed))],
            ignore_index=True,
        )

    try:
        arxiv = fetch_arxiv_papers(args.arxiv)
    except Exception:
        arxiv = generate_paper_fallback("arXiv", args.arxiv)
    arxiv = select_relevant_papers(arxiv, args.arxiv)
    if len(arxiv) < args.arxiv:
        arxiv = pd.concat(
            [arxiv, generate_paper_fallback("arXiv", args.arxiv - len(arxiv))],
            ignore_index=True,
        )

    try:
        nature = fetch_crossref_journal_papers("Nature", args.nature)
    except Exception:
        nature = generate_paper_fallback("Nature", args.nature)
    nature = select_relevant_papers(nature, args.nature)

    try:
        materials_today = fetch_crossref_journal_papers("Materials Today", args.materials_today)
    except Exception:
        materials_today = generate_paper_fallback("Materials Today", args.materials_today)
    materials_today = select_relevant_papers(materials_today, args.materials_today)

    papers = pd.concat([pubmed, arxiv, nature, materials_today], ignore_index=True)
    entities = extract_entities_from_papers(papers)
    return papers, entities


def write_lake_partition(frame: pd.DataFrame, source_name: str, run_id: str) -> Path:
    destination = LAKE_DIR / f"source={source_name}" / f"run_id={run_id}"
    destination.mkdir(parents=True, exist_ok=True)
    output_path = destination / "part-000.parquet"
    normalize_frame(frame).to_parquet(output_path, index=False)
    return output_path


def save_raw_snapshot(name: str, frame: pd.DataFrame) -> None:
    frame.to_csv(RAW_DIR / f"{name}.csv", index=False)


def publish_event(cache_backend, payload: dict[str, object]) -> None:
    try:
        cache_backend.publish_event(REDIS_STREAM_KEY, payload)
    except Exception:
        return None


def upload_artifact_if_enabled(
    storage_client,
    local_path: str | Path,
    run_id: str,
) -> dict[str, str] | None:
    if storage_client is None:
        return None
    path = Path(local_path)
    if not path.exists():
        return None

    object_key = storage_client.build_object_key(path, run_id=run_id)
    object_uri = storage_client.upload_file(path, object_key=object_key)
    return {
        "local_path": str(path),
        "object_key": object_key,
        "object_uri": object_uri,
    }


def write_combined_outputs(
    sources: dict[str, pd.DataFrame],
    run_id: str,
    refresh_vectors: bool,
    storage_client,
    source_artifact_uploads: list[dict[str, str]] | None = None,
    literature_papers: pd.DataFrame | None = None,
) -> dict[str, object]:
    combined = concat_frames(list(sources.values()))
    combined.to_parquet(PARQUET_PATH, index=False)

    validation_results = run_duckdb_validation(PARQUET_PATH, DUCKDB_PATH)
    validation_results.to_csv(VALIDATION_CSV_PATH, index=False)

    build_summary = {
        "rows_total": int(len(combined)),
        "rows_by_type": combined["entity_type"].value_counts(dropna=False).to_dict(),
        "sources": sorted(set(combined["source"].dropna().astype(str).tolist())),
        "parquet_path": str(PARQUET_PATH),
        "duckdb_path": str(DUCKDB_PATH),
        "validation_csv_path": str(VALIDATION_CSV_PATH),
        "validation_rows": int(len(validation_results)),
    }
    BUILD_SUMMARY_PATH.write_text(json.dumps(build_summary, indent=2), encoding="utf-8")

    if literature_papers is not None:
        literature_papers.to_csv(LITERATURE_PAPERS_RAW_PATH, index=False)
    if "literature_entities" in sources:
        sources["literature_entities"].to_parquet(LITERATURE_ENTITY_PATH, index=False)

    vector_summary = None
    if refresh_vectors:
        vector_summary = refresh_vector_assets(parquet_path=PARQUET_PATH)

    object_storage_artifacts: list[dict[str, str]] = []
    if source_artifact_uploads:
        object_storage_artifacts.extend(source_artifact_uploads)

    for artifact_path in [
        PARQUET_PATH,
        DUCKDB_PATH,
        VALIDATION_CSV_PATH,
        BUILD_SUMMARY_PATH,
        LITERATURE_PAPERS_RAW_PATH if literature_papers is not None else None,
        LITERATURE_ENTITY_PATH if "literature_entities" in sources else None,
    ]:
        if artifact_path is None:
            continue
        uploaded = upload_artifact_if_enabled(storage_client, artifact_path, run_id=run_id)
        if uploaded is not None:
            object_storage_artifacts.append(uploaded)

    summary = {
        "run_id": run_id,
        "rows_total": int(len(combined)),
        "rows_by_type": combined["entity_type"].value_counts(dropna=False).to_dict(),
        "sources_ingested": {
            source_name: int(len(frame)) for source_name, frame in sources.items()
        },
        "refresh_vectors": bool(refresh_vectors),
        "vector_summary": vector_summary,
        "lake_dir": str(LAKE_DIR),
        "parquet_path": str(PARQUET_PATH),
        "object_storage_enabled": bool(storage_client is not None),
        "object_storage_artifacts": object_storage_artifacts,
    }
    INGESTION_SUMMARY_PATH.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    return summary


def main() -> None:
    args = parse_args()
    ensure_directories()
    cache_backend = build_cache_backend()
    storage_client = build_object_storage_client()
    state_store = PipelineStateStore()
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    state_store.start_run(
        run_id,
        metadata={
            "include_literature": bool(args.include_literature),
            "refresh_vectors": bool(args.refresh_vectors),
            "max_workers": int(args.max_workers),
            "data_dir": str(DATA_DIR),
            "object_storage_enabled": bool(storage_client is not None),
        },
    )

    publish_event(
        cache_backend,
        {"event": "ingestion_started", "run_id": run_id, "data_dir": str(DATA_DIR)},
    )

    jobs = {
        "genes": load_genes,
        "genbank": load_genbank,
        "materials": load_materials,
        "aflow_materials": load_aflow_materials,
        "oqmd_materials": load_oqmd_materials,
        "nasa_materials": load_nasa_materials,
        "molecules": load_molecules,
        "zinc20": load_zinc20,
        "proteins": load_proteins,
        "alphafold": load_alphafold,
        "boltz1": load_boltz1,
        "structures": load_structures,
        "chembl": load_chembl,
        "nist": load_nist,
        "openfoam": load_openfoam,
        "soil_samples": load_soil,
        "simulations": load_simulations,
    }
    if args.include_literature:
        jobs["literature"] = load_literature

    sources: dict[str, pd.DataFrame] = {}
    literature_papers: pd.DataFrame | None = None
    source_artifact_uploads: list[dict[str, str]] = []

    try:
        with ThreadPoolExecutor(max_workers=max(int(args.max_workers), 1)) as executor:
            future_map = {
                executor.submit(job, args): job_name for job_name, job in jobs.items()
            }
            for future in as_completed(future_map):
                job_name = future_map[future]
                result = future.result()
                if job_name == "literature":
                    papers, entities = result
                    literature_papers = papers
                    sources["literature_entities"] = entities
                    artifact_path = write_lake_partition(entities, "literature_entities", run_id=run_id)
                    state_store.mark_source_complete(
                        source_name="literature_entities",
                        run_id=run_id,
                        row_count=len(entities),
                        artifact_path=str(artifact_path),
                        metadata={"paper_count": len(papers)},
                    )
                    uploaded = upload_artifact_if_enabled(
                        storage_client,
                        artifact_path,
                        run_id=run_id,
                    )
                    if uploaded is not None:
                        source_artifact_uploads.append(uploaded)
                    publish_event(
                        cache_backend,
                        {
                            "event": "source_completed",
                            "run_id": run_id,
                            "source": "literature_entities",
                            "rows": len(entities),
                        },
                    )
                    continue

                frame = result
                sources[job_name] = frame
                artifact_path = write_lake_partition(frame, job_name, run_id=run_id)
                save_raw_snapshot(job_name, frame)
                state_store.mark_source_complete(
                    source_name=job_name,
                    run_id=run_id,
                    row_count=len(frame),
                    artifact_path=str(artifact_path),
                )
                uploaded = upload_artifact_if_enabled(
                    storage_client,
                    artifact_path,
                    run_id=run_id,
                )
                if uploaded is not None:
                    source_artifact_uploads.append(uploaded)
                publish_event(
                    cache_backend,
                    {
                        "event": "source_completed",
                        "run_id": run_id,
                        "source": job_name,
                        "rows": len(frame),
                    },
                )

        summary = write_combined_outputs(
            sources=sources,
            run_id=run_id,
            refresh_vectors=args.refresh_vectors,
            storage_client=storage_client,
            source_artifact_uploads=source_artifact_uploads,
            literature_papers=literature_papers,
        )
        state_store.finish_run(
            run_id=run_id,
            status="completed",
            rows_total=summary["rows_total"],
            rows_by_type=summary["rows_by_type"],
        )
        publish_event(
            cache_backend,
            {"event": "ingestion_completed", "run_id": run_id, "rows_total": summary["rows_total"]},
        )
        print(json.dumps(summary, indent=2))
    except Exception as error:
        state_store.finish_run(run_id=run_id, status="failed", notes=str(error))
        publish_event(
            cache_backend,
            {"event": "ingestion_failed", "run_id": run_id, "error": str(error)},
        )
        raise


if __name__ == "__main__":
    main()

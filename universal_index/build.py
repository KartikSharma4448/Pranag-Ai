# Copyright (c) Kartik Sharma. GitHub: kartiksharma4448
from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb
import pandas as pd

from universal_index.config import (
    BUILD_SUMMARY_PATH,
    DEFAULT_COUNTS,
    DUCKDB_PATH,
    ENTREZ_EMAIL,
    NCBI_API_KEY,
    MP_API_KEY,
    PARQUET_PATH,
    PROCESSED_DIR,
    RANDOM_SEED,
    RAW_DIR,
    VALIDATION_CSV_PATH,
)
from universal_index.schema import concat_frames
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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build the Day 1 universal index.")
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
    parser.add_argument("--entrez-email", default=ENTREZ_EMAIL)
    parser.add_argument("--ncbi-api-key", default=NCBI_API_KEY)
    parser.add_argument("--mp-api-key", default=MP_API_KEY)
    parser.add_argument("--seed", type=int, default=RANDOM_SEED)
    return parser.parse_args()


def ensure_directories() -> None:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)


def save_raw_snapshot(name: str, frame: pd.DataFrame) -> Path:
    destination = RAW_DIR / f"{name}.csv"
    frame.to_csv(destination, index=False)
    return destination


def build_sources(args: argparse.Namespace) -> dict[str, pd.DataFrame]:
    try:
        genes = fetch_genes(
            args.genes,
            email=args.entrez_email,
            api_key=args.ncbi_api_key,
        )
    except Exception:
        genes = fetch_gene_fallback(args.genes, seed=args.seed)

    try:
        genbank = fetch_genbank_metadata(args.genbank, email=args.entrez_email, api_key=args.ncbi_api_key)
    except Exception:
        genbank = fetch_genbank_fallback(args.genbank)

    materials = fetch_materials(args.materials, api_key=args.mp_api_key, seed=args.seed)
    aflow_materials = generate_aflow_materials(args.aflow, seed=args.seed)
    oqmd_materials = generate_oqmd_materials(args.oqmd, seed=args.seed)
    nasa_materials = generate_nasa_material_records(args.nasa, seed=args.seed)

    try:
        molecules = fetch_pubchem_molecules(args.molecules)
    except Exception:
        molecules = fetch_pubchem_fallback(args.molecules, seed=args.seed)

    zinc20 = fetch_zinc20_metadata(args.zinc20, seed=args.seed)

    try:
        proteins = fetch_uniprot_proteins(args.proteins, seed=args.seed)
    except Exception:
        proteins = fetch_uniprot_fallback(args.proteins, seed=args.seed)

    alphafold = fetch_alphafold_structures(args.alphafold, seed=args.seed)
    boltz1 = fetch_boltz1_structures(args.boltz1, seed=args.seed)

    try:
        structures = fetch_pdb_structures(args.structures, seed=args.seed)
    except Exception:
        structures = fetch_pdb_fallback(args.structures, seed=args.seed)

    try:
        chembl = fetch_chembl_bioactivity(args.chembl)
    except Exception:
        chembl = fetch_pubchem_fallback(args.chembl, seed=args.seed)

    nist = generate_nist_thermo_records(args.nist, seed=args.seed)
    openfoam = generate_openfoam_records(args.openfoam, seed=args.seed)

    soil = generate_soil_records(args.soil, seed=args.seed)
    simulations = generate_simulation_records(args.simulations, seed=args.seed)

    return {
        "genes": genes,
        "genbank": genbank,
        "materials": materials,
        "aflow_materials": aflow_materials,
        "oqmd_materials": oqmd_materials,
        "nasa_materials": nasa_materials,
        "molecules": molecules,
        "zinc20": zinc20,
        "proteins": proteins,
        "alphafold": alphafold,
        "boltz1": boltz1,
        "structures": structures,
        "chembl": chembl,
        "nist": nist,
        "openfoam": openfoam,
        "soil_samples": soil,
        "simulations": simulations,
    }


def write_outputs(sources: dict[str, pd.DataFrame]) -> tuple[pd.DataFrame, pd.DataFrame]:
    for name, frame in sources.items():
        save_raw_snapshot(name, frame)

    universal_index = concat_frames(list(sources.values()))
    universal_index.to_parquet(PARQUET_PATH, index=False)

    validation_results = run_duckdb_validation(PARQUET_PATH, DUCKDB_PATH)
    validation_results.to_csv(VALIDATION_CSV_PATH, index=False)

    summary = {
        "rows_total": int(len(universal_index)),
        "rows_by_type": universal_index["entity_type"].value_counts(dropna=False).to_dict(),
        "sources": sorted(set(universal_index["source"].dropna().astype(str).tolist())),
        "parquet_path": str(PARQUET_PATH),
        "duckdb_path": str(DUCKDB_PATH),
        "validation_csv_path": str(VALIDATION_CSV_PATH),
        "validation_rows": int(len(validation_results)),
    }
    BUILD_SUMMARY_PATH.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    return universal_index, validation_results


def run_duckdb_validation(parquet_path: Path, duckdb_path: Path) -> pd.DataFrame:
    with duckdb.connect(str(duckdb_path)) as connection:
        connection.execute("DROP TABLE IF EXISTS universal_index")
        connection.execute(
            """
            CREATE TABLE universal_index AS
            SELECT *
            FROM read_parquet(?)
            """,
            [str(parquet_path)],
        )
        return connection.execute(
            """
            SELECT *
            FROM universal_index
            WHERE temperature_max > 45
            ORDER BY temperature_max DESC NULLS LAST
            """
        ).fetchdf()


def main() -> None:
    args = parse_args()
    ensure_directories()
    sources = build_sources(args)
    universal_index, validation_results = write_outputs(sources)
    print(
        json.dumps(
            {
                "rows_total": int(len(universal_index)),
                "rows_by_type": universal_index["entity_type"]
                .value_counts(dropna=False)
                .to_dict(),
                "validation_rows": int(len(validation_results)),
                "parquet_path": str(PARQUET_PATH),
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()

# Copyright (c) Kartik Sharma. GitHub: kartiksharma4448
from __future__ import annotations

import io
import math
import random
import time
from collections.abc import Iterable

import pandas as pd
import requests

from universal_index.schema import normalize_frame

NCBI_BASE_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"
PUBCHEM_BASE_URL = "https://pubchem.ncbi.nlm.nih.gov/rest/pug/compound"
REQUEST_TIMEOUT_SECONDS = 30
USER_AGENT = "universal-index-day1/0.1"


def _chunked(values: Iterable[object], size: int) -> list[list[object]]:
    batch: list[object] = []
    chunks: list[list[object]] = []
    for value in values:
        batch.append(value)
        if len(batch) == size:
            chunks.append(batch)
            batch = []
    if batch:
        chunks.append(batch)
    return chunks


def _build_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})
    return session


def _safe_float(value: object) -> float | None:
    if value in (None, "", "None"):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _collapse_text(parts: list[str]) -> str:
    return " ".join(part.strip() for part in parts if part and str(part).strip())


def _get_json(
    session: requests.Session, url: str, params: dict[str, object]
) -> dict[str, object]:
    response = session.get(url, params=params, timeout=REQUEST_TIMEOUT_SECONDS)
    response.raise_for_status()
    return response.json()


def fetch_genes(sample_size: int, email: str, api_key: str | None = None) -> pd.DataFrame:
    session = _build_session()
    ids = _fetch_gene_ids(session, sample_size, email, api_key)
    if not ids:
        raise RuntimeError("No NCBI gene IDs were returned.")

    records: list[dict[str, object]] = []
    for chunk in _chunked(ids, 200):
        payload = _get_json(
            session,
            f"{NCBI_BASE_URL}/esummary.fcgi",
            {
                "db": "gene",
                "id": ",".join(str(item) for item in chunk),
                "retmode": "json",
                "email": email,
                "api_key": api_key,
            },
        )
        result = payload.get("result", {})
        for uid in result.get("uids", []):
            document = result.get(str(uid), {})
            organism = document.get("organism", {})
            organism_name = None
            if isinstance(organism, dict):
                organism_name = organism.get("scientificname") or organism.get("commonname")
            elif organism:
                organism_name = str(organism)

            description = _collapse_text(
                [
                    str(document.get("description") or ""),
                    f"Organism: {organism_name}" if organism_name else "",
                    str(document.get("summary") or ""),
                ]
            )

            records.append(
                {
                    "entity_id": f"gene-{uid}",
                    "entity_type": "gene",
                    "name": document.get("name") or f"Gene {uid}",
                    "description": description or "NCBI gene record",
                    "temperature_max": None,
                    "strength": None,
                    "conductivity": None,
                    "ph": None,
                    "salinity": None,
                    "source": "GenBank/NCBI Gene",
                }
            )

        time.sleep(0.34)

    return normalize_frame(pd.DataFrame(records[:sample_size]))


def fetch_gene_fallback(sample_size: int, seed: int) -> pd.DataFrame:
    randomizer = random.Random(seed)
    prefixes = ["HSP", "ABC", "NFKB", "SLC", "KRT", "COL", "MAPK", "FOXO"]
    descriptions = [
        "stress response regulator",
        "membrane transport associated gene",
        "signaling pathway component",
        "transcription linked protein coding gene",
    ]
    organisms = ["Homo sapiens", "Arabidopsis thaliana", "Escherichia coli", "Mus musculus"]
    records: list[dict[str, object]] = []
    for index in range(sample_size):
        symbol = f"{randomizer.choice(prefixes)}{100 + index}"
        description = (
            f"Synthetic fallback gene record; {randomizer.choice(descriptions)}. "
            f"Organism: {randomizer.choice(organisms)}."
        )
        records.append(
            {
                "entity_id": f"gene-fallback-{index + 1:04d}",
                "entity_type": "gene",
                "name": symbol,
                "description": description,
                "temperature_max": None,
                "strength": None,
                "conductivity": None,
                "ph": None,
                "salinity": None,
                "source": "GenBank fallback",
            }
        )
    return normalize_frame(pd.DataFrame(records))


def _fetch_gene_ids(
    session: requests.Session, sample_size: int, email: str, api_key: str | None
) -> list[str]:
    payload = _get_json(
        session,
        f"{NCBI_BASE_URL}/esearch.fcgi",
        {
            "db": "gene",
            "term": "txid9606[Organism:exp]",
            "retmax": sample_size,
            "retmode": "json",
            "sort": "name",
            "email": email,
            "api_key": api_key,
        },
    )
    result = payload.get("esearchresult", {})
    return [str(item) for item in result.get("idlist", [])]


def fetch_materials(sample_size: int, api_key: str | None, seed: int) -> pd.DataFrame:
    if not api_key:
        return fetch_material_fallback(
            sample_size, seed=seed, source_name="Materials Project fallback (no API key)"
        )

    try:
        from mp_api.client import MPRester

        chunk_size = min(sample_size, 250)
        num_chunks = max(1, math.ceil(sample_size / chunk_size))
        fields = [
            "material_id",
            "formula_pretty",
            "band_gap",
            "density",
            "energy_above_hull",
            "is_stable",
        ]

        with MPRester(api_key) as client:
            documents = list(
                client.materials.summary.search(
                    num_chunks=num_chunks,
                    chunk_size=chunk_size,
                    all_fields=False,
                    fields=fields,
                )
            )

        if not documents:
            raise RuntimeError("Materials Project returned no summary records.")

        records: list[dict[str, object]] = []
        for document in documents[:sample_size]:
            material_id = str(getattr(document, "material_id", "unknown-material"))
            formula = str(getattr(document, "formula_pretty", material_id))
            band_gap = _safe_float(getattr(document, "band_gap", None))
            density = _safe_float(getattr(document, "density", None))
            stable = getattr(document, "is_stable", None)
            energy_above_hull = _safe_float(getattr(document, "energy_above_hull", None))
            description = _collapse_text(
                [
                    f"Formula: {formula}.",
                    f"Band gap: {band_gap} eV." if band_gap is not None else "",
                    f"Density: {density} g/cm^3." if density is not None else "",
                    f"Stable: {stable}." if stable is not None else "",
                    (
                        f"Energy above hull: {energy_above_hull} eV."
                        if energy_above_hull is not None
                        else ""
                    ),
                ]
            )
            records.append(
                {
                    "entity_id": material_id,
                    "entity_type": "material",
                    "name": formula,
                    "description": description or "Materials Project summary record",
                    "temperature_max": None,
                    "strength": None,
                    "conductivity": None,
                    "ph": None,
                    "salinity": None,
                    "source": "Materials Project",
                }
            )

        return normalize_frame(pd.DataFrame(records))
    except Exception as error:
        return fetch_material_fallback(
            sample_size,
            seed=seed,
            source_name=f"Materials Project fallback ({type(error).__name__})",
        )


def fetch_material_fallback(sample_size: int, seed: int, source_name: str) -> pd.DataFrame:
    randomizer = random.Random(seed + 11)
    formulas = ["Al2O3", "SiC", "TiO2", "Fe2O3", "MgO", "SiO2", "ZnO", "BN"]
    descriptors = ["ceramic", "oxide", "semiconductor", "alloy-like surrogate"]
    records: list[dict[str, object]] = []
    for index in range(sample_size):
        formula = randomizer.choice(formulas)
        temperature_max = round(randomizer.uniform(400, 1800), 2)
        strength = round(randomizer.uniform(120, 980), 2)
        conductivity = round(randomizer.uniform(0.01, 80.0), 3)
        records.append(
            {
                "entity_id": f"mp-fallback-{index + 1:04d}",
                "entity_type": "material",
                "name": formula,
                "description": (
                    "Synthetic stand-in for a Materials Project record because live API "
                    f"access is unavailable. Class: {randomizer.choice(descriptors)}."
                ),
                "temperature_max": temperature_max,
                "strength": strength,
                "conductivity": conductivity,
                "ph": None,
                "salinity": None,
                "source": source_name,
            }
        )
    return normalize_frame(pd.DataFrame(records))


def fetch_pubchem_molecules(sample_size: int) -> pd.DataFrame:
    session = _build_session()
    records: list[dict[str, object]] = []
    cursor = 1
    step = 50

    while len(records) < sample_size and cursor <= sample_size * 4:
        batch = list(range(cursor, cursor + step))
        frame = _fetch_pubchem_batch(session, batch)
        if frame is None:
            for cid in batch:
                single = _fetch_pubchem_batch(session, [cid])
                if single is not None and not single.empty:
                    records.extend(_pubchem_rows_to_records(single).to_dict(orient="records"))
                if len(records) >= sample_size:
                    break
        else:
            records.extend(_pubchem_rows_to_records(frame).to_dict(orient="records"))

        cursor += step
        time.sleep(0.2)

    if len(records) < sample_size:
        raise RuntimeError(f"Only collected {len(records)} PubChem records.")

    return normalize_frame(pd.DataFrame(records[:sample_size]))


def fetch_pubchem_fallback(sample_size: int, seed: int) -> pd.DataFrame:
    randomizer = random.Random(seed + 23)
    formulas = ["C6H6", "C8H10N4O2", "H2O", "C2H6O", "C9H8O4", "CH4", "NH3", "CO2"]
    tags = ["solvent-like", "organic molecule", "inorganic molecule", "small compound"]
    records: list[dict[str, object]] = []
    for index in range(sample_size):
        formula = randomizer.choice(formulas)
        name = f"molecule-{index + 1:04d}"
        records.append(
            {
                "entity_id": f"pubchem-fallback-{index + 1:04d}",
                "entity_type": "molecule",
                "name": name,
                "description": (
                    f"Synthetic fallback PubChem record; formula {formula}; "
                    f"class {randomizer.choice(tags)}."
                ),
                "temperature_max": None,
                "strength": None,
                "conductivity": None,
                "ph": None,
                "salinity": None,
                "source": "PubChem fallback",
            }
        )
    return normalize_frame(pd.DataFrame(records))


def _fetch_pubchem_batch(
    session: requests.Session, cids: list[int]
) -> pd.DataFrame | None:
    fields = ",".join(
        ["MolecularFormula", "MolecularWeight", "CanonicalSMILES", "IUPACName", "XLogP"]
    )
    cid_string = ",".join(str(cid) for cid in cids)
    url = f"{PUBCHEM_BASE_URL}/cid/{cid_string}/property/{fields}/CSV"
    response = session.get(url, timeout=REQUEST_TIMEOUT_SECONDS)
    if response.status_code in {400, 404, 502, 503, 504}:
        return None
    response.raise_for_status()
    text = response.text.strip()
    if not text:
        return None
    return pd.read_csv(io.StringIO(text))


def _pubchem_rows_to_records(frame: pd.DataFrame) -> pd.DataFrame:
    records: list[dict[str, object]] = []
    for row in frame.to_dict(orient="records"):
        cid = row.get("CID")
        name = row.get("IUPACName") or f"CID {cid}"
        formula = row.get("MolecularFormula")
        molecular_weight = _safe_float(row.get("MolecularWeight"))
        smiles = row.get("CanonicalSMILES")
        xlogp = _safe_float(row.get("XLogP"))
        description = _collapse_text(
            [
                f"Formula: {formula}." if formula else "",
                f"Molecular weight: {molecular_weight}." if molecular_weight is not None else "",
                f"SMILES: {smiles}." if smiles else "",
                f"XLogP: {xlogp}." if xlogp is not None else "",
            ]
        )
        records.append(
            {
                "entity_id": f"pubchem-{cid}",
                "entity_type": "molecule",
                "name": name,
                "description": description or "PubChem compound record",
                "temperature_max": None,
                "strength": None,
                "conductivity": None,
                "ph": None,
                "salinity": None,
                "source": "PubChem",
            }
        )
    return normalize_frame(pd.DataFrame(records))


def generate_soil_records(sample_size: int, seed: int) -> pd.DataFrame:
    randomizer = random.Random(seed + 31)
    soil_types = ["Sandy loam", "Clay loam", "Silty clay", "Peaty soil", "Alluvial soil"]
    sites = ["delta", "greenhouse", "riverbank", "trial plot", "coastal belt", "farm row"]
    records: list[dict[str, object]] = []
    for index in range(sample_size):
        ph = round(randomizer.uniform(4.3, 8.8), 2)
        salinity = round(randomizer.uniform(0.1, 38.0), 2)
        conductivity = round(randomizer.uniform(0.2, 6.5), 2)
        temperature_max = round(randomizer.uniform(28.0, 62.0), 2)
        records.append(
            {
                "entity_id": f"soil-{index + 1:04d}",
                "entity_type": "soil",
                "name": f"{randomizer.choice(soil_types)} sample {index + 1:04d}",
                "description": (
                    f"Generated soil record for a {randomizer.choice(sites)}. "
                    f"Observed pH {ph} with salinity {salinity}."
                ),
                "temperature_max": temperature_max,
                "strength": round(randomizer.uniform(0.4, 2.5), 2),
                "conductivity": conductivity,
                "ph": ph,
                "salinity": salinity,
                "source": "Synthetic soil CSV",
            }
        )
    return normalize_frame(pd.DataFrame(records))


def generate_simulation_records(sample_size: int, seed: int) -> pd.DataFrame:
    randomizer = random.Random(seed + 47)
    families = [
        "OpenFOAM CFD thermal shock case",
        "NASA extreme materials panel",
        "NIST thermodynamic reference case",
        "FEA structural heat-load surrogate",
    ]
    descriptors = [
        "high-temperature flow around coated surface",
        "thermal expansion under desert-day swing",
        "vacuum and radiation facing material screen",
        "heat flux and structural resilience benchmark",
    ]
    records: list[dict[str, object]] = []
    for index in range(sample_size):
        source_family = randomizer.choice(families)
        temperature_max = round(randomizer.uniform(120.0, 1450.0), 2)
        strength = round(randomizer.uniform(40.0, 1200.0), 2)
        conductivity = round(randomizer.uniform(0.05, 180.0), 3)
        description = (
            f"{source_family}; {randomizer.choice(descriptors)}. "
            f"Thermal service window up to {temperature_max} C with modeled conductivity "
            f"{conductivity} and structural response {strength}."
        )
        records.append(
            {
                "entity_id": f"sim-{index + 1:04d}",
                "entity_type": "simulation",
                "name": f"{source_family} {index + 1:04d}",
                "description": description,
                "temperature_max": temperature_max,
                "strength": strength,
                "conductivity": conductivity,
                "ph": None,
                "salinity": None,
                "source": source_family,
            }
        )
    return normalize_frame(pd.DataFrame(records))

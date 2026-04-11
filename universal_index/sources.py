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
UNIPROT_BASE_URL = "https://rest.uniprot.org/uniprotkb/search"
RCSB_ENTRY_IDS_URL = "https://data.rcsb.org/rest/v1/holdings/current/entry_ids"
RCSB_ENTRY_URL = "https://data.rcsb.org/rest/v1/core/entry"
CHEMBL_ACTIVITY_URL = "https://www.ebi.ac.uk/chembl/api/data/activity.json"
REQUEST_TIMEOUT_SECONDS = 30
USER_AGENT = "universal-index-day1/0.1"
HOMO_SAPIENS = "Homo sapiens"
ARABIDOPSIS_THALIANA = "Arabidopsis thaliana"
ESCHERICHIA_COLI = "Escherichia coli"
MUS_MUSCULUS = "Mus musculus"
COMMON_ORGANISMS = [HOMO_SAPIENS, ARABIDOPSIS_THALIANA, ESCHERICHIA_COLI, MUS_MUSCULUS]


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
            records.append(_gene_document_to_record(uid=uid, document=document))

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
    records: list[dict[str, object]] = []
    for index in range(sample_size):
        symbol = f"{randomizer.choice(prefixes)}{100 + index}"
        description = (
            f"Synthetic fallback gene record; {randomizer.choice(descriptions)}. "
            f"Organism: {randomizer.choice(COMMON_ORGANISMS)}."
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
                "confidence": 0.60,
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
                    "confidence": 0.88,
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
                "confidence": 0.60,
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
        records.extend(_collect_pubchem_records(session, batch, sample_size=sample_size, current_count=len(records)))

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
                "confidence": 0.60,
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
                "confidence": 0.88,
                "source": "PubChem",
            }
        )
    return normalize_frame(pd.DataFrame(records))


def _collect_pubchem_records(
    session: requests.Session,
    batch: list[int],
    sample_size: int,
    current_count: int,
) -> list[dict[str, object]]:
    collected: list[dict[str, object]] = []
    frame = _fetch_pubchem_batch(session, batch)
    if frame is None:
        for cid in batch:
            if current_count + len(collected) >= sample_size:
                break
            single = _fetch_pubchem_batch(session, [cid])
            if single is not None and not single.empty:
                collected.extend(_pubchem_rows_to_records(single).to_dict(orient="records"))
        return collected

    collected.extend(_pubchem_rows_to_records(frame).to_dict(orient="records"))
    return collected


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
                "confidence": 0.55,
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
                "confidence": 0.55,
                "source": source_family,
            }
        )
    return normalize_frame(pd.DataFrame(records))


def fetch_uniprot_proteins(sample_size: int, seed: int) -> pd.DataFrame:
    session = _build_session()
    params = {
        "query": "reviewed:true AND organism_id:9606",
        "format": "tsv",
        "fields": "accession,id,gene_primary,protein_name,organism_name,cc_function,xref_pdb",
        "size": sample_size,
    }

    try:
        response = session.get(UNIPROT_BASE_URL, params=params, timeout=REQUEST_TIMEOUT_SECONDS)
        response.raise_for_status()
        frame = pd.read_csv(io.StringIO(response.text), sep="\t")
        return _uniprot_rows_to_frame(frame, sample_size)
    except Exception:
        return fetch_uniprot_fallback(sample_size, seed=seed)


def fetch_uniprot_fallback(sample_size: int, seed: int) -> pd.DataFrame:
    randomizer = random.Random(seed + 59)
    prefixes = ["CA", "HSP", "SOD", "KIN", "ATP", "POT", "ABC", "COL"]
    functions = [
        "stress response protein",
        "membrane transporter",
        "enzymatic regulator",
        "signal transduction protein",
    ]
    organisms = [HOMO_SAPIENS, ARABIDOPSIS_THALIANA, "Bacillus subtilis", MUS_MUSCULUS]
    records: list[dict[str, object]] = []
    for index in range(sample_size):
        gene = f"{randomizer.choice(prefixes)}{1000 + index}"
        records.append(
            {
                "entity_id": f"uniprot-fallback-{index + 1:04d}",
                "entity_type": "protein",
                "name": gene,
                "description": (
                    f"Synthetic UniProt fallback record. Protein family: {randomizer.choice(functions)}. "
                    f"Organism: {randomizer.choice(organisms)}."
                ),
                "temperature_max": None,
                "strength": None,
                "conductivity": None,
                "ph": None,
                "salinity": None,
                "confidence": 0.60,
                "source": "UniProt fallback",
            }
        )
    return normalize_frame(pd.DataFrame(records))


def fetch_pdb_structures(sample_size: int, seed: int) -> pd.DataFrame:
    session = _build_session()
    try:
        ids_response = session.get(RCSB_ENTRY_IDS_URL, timeout=REQUEST_TIMEOUT_SECONDS)
        ids_response.raise_for_status()
        entry_ids = _extract_entry_ids(ids_response.json())
        if not entry_ids:
            raise RuntimeError("RCSB did not return any PDB entry identifiers.")

        randomizer = random.Random(seed + 71)
        selected_ids = _sample_ids(entry_ids, sample_size=sample_size, randomizer=randomizer)
        records: list[dict[str, object]] = []
        for pdb_id in selected_ids:
            entry_response = session.get(
                f"{RCSB_ENTRY_URL}/{pdb_id}",
                timeout=REQUEST_TIMEOUT_SECONDS,
            )
            entry_response.raise_for_status()
            records.append(_rcsb_entry_to_record(pdb_id, entry_response.json()))

        return normalize_frame(pd.DataFrame(records))
    except Exception:
        return fetch_pdb_fallback(sample_size, seed=seed)


def fetch_pdb_fallback(sample_size: int, seed: int) -> pd.DataFrame:
    randomizer = random.Random(seed + 73)
    families = ["enzyme structure", "membrane protein", "metal-binding protein", "signal complex"]
    organisms = [HOMO_SAPIENS, ESCHERICHIA_COLI, "Thermus thermophilus", ARABIDOPSIS_THALIANA]
    records: list[dict[str, object]] = []
    for index in range(sample_size):
        pdb_id = f"{randomizer.choice(['1', '2', '3', '4', '5'])}{randomizer.choice('abcdefghijklmnopqrstuvwxyz')}{randomizer.choice('abcdefghijklmnopqrstuvwxyz')}{index % 10}"
        records.append(
            {
                "entity_id": f"pdb-fallback-{index + 1:04d}",
                "entity_type": "structure",
                "name": pdb_id.upper(),
                "description": (
                    f"Synthetic PDB fallback record. Structure family: {randomizer.choice(families)}. "
                    f"Organism: {randomizer.choice(organisms)}."
                ),
                "temperature_max": None,
                "strength": None,
                "conductivity": None,
                "ph": None,
                "salinity": None,
                "confidence": 0.60,
                "source": "RCSB PDB fallback",
            }
        )
    return normalize_frame(pd.DataFrame(records))


def fetch_chembl_bioactivity(sample_size: int) -> pd.DataFrame:
    session = _build_session()
    params = {
        "limit": sample_size,
        "format": "json",
    }

    try:
        response = session.get(CHEMBL_ACTIVITY_URL, params=params, timeout=REQUEST_TIMEOUT_SECONDS)
        response.raise_for_status()
        payload = response.json()
        records = payload.get("activities", []) if isinstance(payload, dict) else []
        frame = _chembl_activity_rows_to_frame(records)
        if frame.empty:
            raise RuntimeError("ChEMBL returned no activities.")
        return normalize_frame(frame)
    except Exception:
        return fetch_chembl_fallback(sample_size)


def fetch_chembl_fallback(sample_size: int) -> pd.DataFrame:
    randomizer = random.Random(101)
    compounds = ["CHEMBL25", "CHEMBL58", "CHEMBL120", "CHEMBL190", "CHEMBL233"]
    records: list[dict[str, object]] = []
    for index in range(sample_size):
        chembl_id = randomizer.choice(compounds)
        records.append(
            {
                "entity_id": f"chembl-fallback-{index + 1:04d}",
                "entity_type": "molecule",
                "name": chembl_id,
                "description": (
                    "Synthetic ChEMBL fallback record with bioactivity-linked chemistry. "
                    "Standard relation: IC50 surrogate; target linkage retained for cross-domain retrieval."
                ),
                "temperature_max": None,
                "strength": None,
                "conductivity": None,
                "ph": None,
                "salinity": None,
                "confidence": 0.60,
                "source": "ChEMBL fallback",
            }
        )
    return normalize_frame(pd.DataFrame(records))


def generate_aflow_materials(sample_size: int, seed: int) -> pd.DataFrame:
    randomizer = random.Random(seed + 83)
    formulas = ["Al2O3", "SiC", "TiN", "Fe3Al", "ZrO2", "HfO2", "BN", "MgAl2O4"]
    records: list[dict[str, object]] = []
    for index in range(sample_size):
        formula = randomizer.choice(formulas)
        records.append(
            {
                "entity_id": f"aflow-{index + 1:04d}",
                "entity_type": "material",
                "name": formula,
                "description": (
                    "AFLOW surrogate material record with crystal-structure and property metadata. "
                    "Useful for conductivity, thermal stability, and band-gap screening."
                ),
                "temperature_max": round(randomizer.uniform(600, 2200), 2),
                "strength": round(randomizer.uniform(180, 1200), 2),
                "conductivity": round(randomizer.uniform(0.01, 120.0), 3),
                "ph": None,
                "salinity": None,
                "confidence": 0.75,
                "source": "AFLOW surrogate",
            }
        )
    return normalize_frame(pd.DataFrame(records))


def generate_oqmd_materials(sample_size: int, seed: int) -> pd.DataFrame:
    randomizer = random.Random(seed + 89)
    formulas = ["GaN", "ZnO", "SrTiO3", "BaTiO3", "SiC", "AlN", "TiO2", "SnO2"]
    records: list[dict[str, object]] = []
    for index in range(sample_size):
        formula = randomizer.choice(formulas)
        records.append(
            {
                "entity_id": f"oqmd-{index + 1:04d}",
                "entity_type": "material",
                "name": formula,
                "description": (
                    "OQMD surrogate material record with thermodynamic stability metadata. "
                    "Useful for phase stability and band-gap retrieval.") ,
                "temperature_max": round(randomizer.uniform(500, 1800), 2),
                "strength": round(randomizer.uniform(100, 900), 2),
                "conductivity": round(randomizer.uniform(0.01, 80.0), 3),
                "ph": None,
                "salinity": None,
                "confidence": 0.75,
                "source": "OQMD surrogate",
            }
        )
    return normalize_frame(pd.DataFrame(records))


def _uniprot_rows_to_frame(frame: pd.DataFrame, sample_size: int) -> pd.DataFrame:
    records: list[dict[str, object]] = []
    for row in frame.to_dict(orient="records")[:sample_size]:
        records.append(_uniprot_row_to_record(row))
    return normalize_frame(pd.DataFrame(records))


def _uniprot_row_to_record(row: dict[str, object]) -> dict[str, object]:
    accession = str(row.get("Entry") or row.get("accession") or "unknown")
    protein_name = str(row.get("Protein names") or row.get("protein_name") or accession)
    gene = str(row.get("Gene Names (primary)") or row.get("gene_primary") or "")
    organism = str(row.get("Organism") or row.get("organism_name") or "")
    function = str(row.get("Function [CC]") or row.get("cc_function") or "")
    pdb_refs = str(row.get("Cross-reference (PDB)") or row.get("xref_pdb") or "")
    description = _collapse_text(
        [
            f"Gene: {gene}." if gene else "",
            f"Organism: {organism}." if organism else "",
            f"Function: {function}." if function else "",
            f"PDB refs: {pdb_refs}." if pdb_refs else "",
        ]
    )
    return {
        "entity_id": f"uniprot-{accession}",
        "entity_type": "protein",
        "name": protein_name,
        "description": description or "UniProt protein record",
        "temperature_max": None,
        "strength": None,
        "conductivity": None,
        "ph": None,
        "salinity": None,
        "confidence": 0.88,
        "source": "UniProt",
    }


def _extract_entry_ids(payload: object) -> list[str]:
    if isinstance(payload, dict):
        for key in ["entry_ids", "ids", "result_set"]:
            candidate = payload.get(key)
            if isinstance(candidate, list):
                return [str(item) for item in candidate if str(item).strip()]
    if isinstance(payload, list):
        return [str(item) for item in payload if str(item).strip()]
    return []


def _sample_ids(entry_ids: list[str], sample_size: int, randomizer: random.Random) -> list[str]:
    if len(entry_ids) <= sample_size:
        return entry_ids[:sample_size]
    indices = sorted(randomizer.sample(range(len(entry_ids)), sample_size))
    return [entry_ids[index] for index in indices]


def _rcsb_entry_to_record(pdb_id: str, payload: dict[str, object]) -> dict[str, object]:
    title = _rcsb_extract_text(payload, "struct", ["title", "pdbx_descriptor"])
    method = _rcsb_extract_list_text(payload, "exptl", ["method"])
    citation = _rcsb_extract_text(payload, "rcsb_primary_citation", ["title", "journal_abbrev"])

    description = _collapse_text(
        [
            f"Title: {title}." if title else "",
            f"Method: {method}." if method else "",
            f"Citation: {citation}." if citation else "",
        ]
    )
    return {
        "entity_id": f"pdb-{pdb_id}",
        "entity_type": "structure",
        "name": pdb_id.upper(),
        "description": description or "RCSB PDB structure record",
        "temperature_max": None,
        "strength": None,
        "conductivity": None,
        "ph": None,
        "salinity": None,
        "confidence": 0.88,
        "source": "RCSB PDB",
    }


def _rcsb_extract_text(payload: dict[str, object], key: str, candidate_keys: list[str]) -> str:
    container = payload.get(key, {}) if isinstance(payload, dict) else {}
    if not isinstance(container, dict):
        return ""
    for candidate_key in candidate_keys:
        value = container.get(candidate_key)
        if value not in (None, ""):
            return str(value)
    return ""


def _rcsb_extract_list_text(payload: dict[str, object], key: str, candidate_keys: list[str]) -> str:
    container = payload.get(key, []) if isinstance(payload, dict) else []
    if not isinstance(container, list) or not container:
        return ""
    first = container[0]
    if not isinstance(first, dict):
        return ""
    for candidate_key in candidate_keys:
        value = first.get(candidate_key)
        if value not in (None, ""):
            return str(value)
    return ""


def _chembl_activity_rows_to_frame(rows: list[dict[str, object]]) -> pd.DataFrame:
    records: list[dict[str, object]] = []
    for index, row in enumerate(rows, start=1):
        molecule_id = str(row.get("molecule_chembl_id") or row.get("molregno") or "chembl-unknown")
        target_id = str(row.get("target_chembl_id") or row.get("target_id") or "")
        activity_id = str(row.get("activity_id") or index)
        standard_type = str(row.get("standard_type") or "")
        standard_value = _safe_float(row.get("standard_value"))
        standard_units = str(row.get("standard_units") or "")
        pchembl_value = _safe_float(row.get("pchembl_value"))
        description = _collapse_text(
            [
                f"Target: {target_id}." if target_id else "",
                f"Assay type: {standard_type}." if standard_type else "",
                (
                    f"Activity: {standard_value} {standard_units}."
                    if standard_value is not None and standard_units
                    else ""
                ),
                f"pChEMBL: {pchembl_value}." if pchembl_value is not None else "",
            ]
        )
        records.append(
            {
                "entity_id": f"chembl-{molecule_id}-{activity_id}",
                "entity_type": "molecule",
                "name": molecule_id,
                "description": description or "ChEMBL bioactivity record",
                "temperature_max": None,
                "strength": None,
                "conductivity": None,
                "ph": None,
                "salinity": None,
                "confidence": 0.88,
                "source": "ChEMBL",
            }
        )
    return pd.DataFrame(records)


def fetch_genbank_metadata(sample_size: int, email: str, api_key: str | None = None) -> pd.DataFrame:
    session = _build_session()
    ids = _fetch_nuccore_ids(session, sample_size=sample_size, email=email, api_key=api_key)
    if not ids:
        return fetch_genbank_fallback(sample_size)

    records: list[dict[str, object]] = []
    for chunk in _chunked(ids, 200):
        payload = _get_json(
            session,
            f"{NCBI_BASE_URL}/esummary.fcgi",
            {
                "db": "nuccore",
                "id": ",".join(str(item) for item in chunk),
                "retmode": "json",
                "email": email,
                "api_key": api_key,
            },
        )
        result = payload.get("result", {})
        for uid in result.get("uids", []):
            document = result.get(str(uid), {})
            records.append(_genbank_document_to_record(uid=uid, document=document))

        time.sleep(0.34)

    return normalize_frame(pd.DataFrame(records[:sample_size]))


def fetch_genbank_fallback(sample_size: int) -> pd.DataFrame:
    randomizer = random.Random(137)
    records: list[dict[str, object]] = []
    for index in range(sample_size):
        accession = f"GB{100000 + index}"
        records.append(
            {
                "entity_id": f"genbank-fallback-{index + 1:04d}",
                "entity_type": "gene",
                "name": accession,
                "description": (
                    f"Synthetic GenBank metadata record for {accession}. "
                    f"Organism: {randomizer.choice(COMMON_ORGANISMS)}. "
                    "Metadata-only sequence and annotation placeholder."
                ),
                "temperature_max": None,
                "strength": None,
                "conductivity": None,
                "ph": None,
                "salinity": None,
                "source": "GenBank fallback",
            }
        )
    return normalize_frame(pd.DataFrame(records))


def fetch_alphafold_structures(sample_size: int, seed: int) -> pd.DataFrame:
    randomizer = random.Random(seed + 97)
    records: list[dict[str, object]] = []
    for index in range(sample_size):
        accession = f"AF-{100000 + index}"
        gene = f"AFGENE{index + 1:04d}"
        records.append(
            {
                "entity_id": f"alphafold-{index + 1:04d}",
                "entity_type": "structure",
                "name": accession,
                "description": (
                    f"AlphaFold predicted protein structure metadata for {gene}. "
                    f"Predicted confidence {round(randomizer.uniform(70, 98), 2)}. "
                    "Store the structure ID, not the raw coordinate file."
                ),
                "temperature_max": None,
                "strength": None,
                "conductivity": None,
                "ph": None,
                "salinity": None,
                "confidence": 0.75,
                "source": "AlphaFold DB surrogate",
            }
        )
    return normalize_frame(pd.DataFrame(records))


def fetch_boltz1_structures(sample_size: int, seed: int) -> pd.DataFrame:
    randomizer = random.Random(seed + 101)
    records: list[dict[str, object]] = []
    for index in range(sample_size):
        complex_id = f"BOLTZ1-{10000 + index}"
        records.append(
            {
                "entity_id": f"boltz1-{index + 1:04d}",
                "entity_type": "structure",
                "name": complex_id,
                "description": (
                    "boltz1 predicted protein complex metadata surrogate. "
                    f"Interface confidence {round(randomizer.uniform(0.55, 0.99), 2)}. "
                    "Captured as structure metadata for downstream retrieval."
                ),
                "temperature_max": None,
                "strength": None,
                "conductivity": None,
                "ph": None,
                "salinity": None,
                "confidence": 0.75,
                "source": "boltz1 surrogate",
            }
        )
    return normalize_frame(pd.DataFrame(records))


def fetch_zinc20_metadata(sample_size: int, seed: int) -> pd.DataFrame:
    randomizer = random.Random(seed + 103)
    scaffolds = ["C1=CC=CC=C1", "CCO", "CCN", "CC(=O)O", "CNC", "COC"]
    records: list[dict[str, object]] = []
    for index in range(sample_size):
        smiles = randomizer.choice(scaffolds)
        records.append(
            {
                "entity_id": f"zinc20-{index + 1:04d}",
                "entity_type": "molecule",
                "name": f"ZINC20-{100000 + index}",
                "description": (
                    f"ZINC20 purchasable compound metadata surrogate. SMILES: {smiles}. "
                    "Availability and medicinal chemistry screening placeholder."
                ),
                "temperature_max": None,
                "strength": None,
                "conductivity": None,
                "ph": None,
                "salinity": None,
                "confidence": 0.75,
                "source": "ZINC20 surrogate",
            }
        )
    return normalize_frame(pd.DataFrame(records))


def generate_nasa_material_records(sample_size: int, seed: int) -> pd.DataFrame:
    randomizer = random.Random(seed + 107)
    records: list[dict[str, object]] = []
    for index in range(sample_size):
        material = randomizer.choice(["Inconel 718", "Ti-6Al-4V", "SiC", "C/C composite", "Al2O3"])
        records.append(
            {
                "entity_id": f"nasa-{index + 1:04d}",
                "entity_type": "material",
                "name": material,
                "description": (
                    "NASA extreme-condition material metadata surrogate with thermal, vacuum, and radiation resistance context. "
                    "Useful for spacecraft and high-temperature screening."
                ),
                "temperature_max": round(randomizer.uniform(250, 2200), 2),
                "strength": round(randomizer.uniform(100, 1600), 2),
                "conductivity": round(randomizer.uniform(0.01, 150.0), 3),
                "ph": None,
                "salinity": None,
                "confidence": 0.75,
                "source": "NASA materials surrogate",
            }
        )
    return normalize_frame(pd.DataFrame(records))


def generate_nist_thermo_records(sample_size: int, seed: int) -> pd.DataFrame:
    randomizer = random.Random(seed + 109)
    records: list[dict[str, object]] = []
    for index in range(sample_size):
        formula = randomizer.choice(["H2O", "CO2", "NH3", "CH4", "SiO2", "Al2O3"])
        records.append(
            {
                "entity_id": f"nist-{index + 1:04d}",
                "entity_type": "simulation",
                "name": formula,
                "description": (
                    "NIST Chemistry WebBook thermodynamic surrogate record with heats of formation, entropy, and heat capacity context. "
                    "Useful for equilibrium, enthalpy, and materials process screening."
                ),
                "temperature_max": round(randomizer.uniform(80, 1800), 2),
                "strength": None,
                "conductivity": None,
                "ph": None,
                "salinity": None,
                "confidence": 0.75,
                "source": "NIST WebBook surrogate",
            }
        )
    return normalize_frame(pd.DataFrame(records))


def generate_openfoam_records(sample_size: int, seed: int) -> pd.DataFrame:
    randomizer = random.Random(seed + 113)
    cases = ["thermal shock", "boundary layer", "heat transfer", "turbulence", "external flow"]
    records: list[dict[str, object]] = []
    for index in range(sample_size):
        case_name = randomizer.choice(cases)
        records.append(
            {
                "entity_id": f"openfoam-{index + 1:04d}",
                "entity_type": "simulation",
                "name": f"OpenFOAM {case_name} {index + 1:04d}",
                "description": (
                    "OpenFOAM CFD template surrogate with standardized mesh, solver, and boundary-condition metadata. "
                    "Useful for pre-run engineering setup retrieval."
                ),
                "temperature_max": round(randomizer.uniform(100, 1600), 2),
                "strength": round(randomizer.uniform(50, 900), 2),
                "conductivity": round(randomizer.uniform(0.05, 200.0), 3),
                "ph": None,
                "salinity": None,
                "confidence": 0.75,
                "source": "OpenFOAM surrogate",
            }
        )
    return normalize_frame(pd.DataFrame(records))


def _fetch_nuccore_ids(
    session: requests.Session,
    sample_size: int,
    email: str,
    api_key: str | None,
) -> list[str]:
    payload = _get_json(
        session,
        f"{NCBI_BASE_URL}/esearch.fcgi",
        {
            "db": "nuccore",
            "term": "txid9606[Organism:exp]",
            "retmax": sample_size,
            "retmode": "json",
            "sort": "date",
            "email": email,
            "api_key": api_key,
        },
    )
    result = payload.get("esearchresult", {})
    return [str(item) for item in result.get("idlist", [])]


def _genbank_document_to_record(uid: str, document: dict[str, object]) -> dict[str, object]:
    accession = str(document.get("accessionversion") or document.get("accession") or uid)
    organism = document.get("organism", {})
    organism_name = None
    if isinstance(organism, dict):
        organism_name = organism.get("scientificname") or organism.get("commonname")
    elif organism:
        organism_name = str(organism)

    description = _collapse_text(
        [
            str(document.get("title") or ""),
            f"Organism: {organism_name}" if organism_name else "",
            f"Length: {document.get('slen')} bp." if document.get("slen") else "",
            f"Update: {document.get('updatedate')}." if document.get("updatedate") else "",
        ]
    )
    return {
        "entity_id": f"genbank-{accession}",
        "entity_type": "gene",
        "name": accession,
        "description": description or "GenBank metadata record",
        "temperature_max": None,
        "strength": None,
        "conductivity": None,
        "ph": None,
        "salinity": None,
        "confidence": 0.88,
        "source": "GenBank",
    }

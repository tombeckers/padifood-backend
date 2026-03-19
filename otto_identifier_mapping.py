from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path

from sqlalchemy import delete, select
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.ext.asyncio import AsyncSession

from models import InvoiceLine, Kloklijst, OttoIdentifierMapping


BASE_DIR = Path(__file__).resolve().parent
DEFAULT_BACKUP_CSV_PATH = (BASE_DIR / "output" / "otto_identifier_mapping_backup.csv").resolve()
CSV_HEADERS = [
    "week",
    "provider",
    "kloklijst_loonnummer",
    "kloklijst_name",
    "factuur_sap_id",
    "factuur_name",
    "match_type",
    "resolved",
    "persistable",
]


def _normalize_name(name: str) -> str:
    return " ".join(sorted(name.lower().replace("-", " ").split()))


def _to_id(value: object | None) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text if text else None


@dataclass
class OttoPerson:
    normalized_name: str
    kloklijst_name: str
    kloklijst_loonnummer: str | None


@dataclass
class FactuurPerson:
    normalized_name: str
    factuur_name: str
    sap_id: str


async def build_otto_mapping_candidates(
    week: int,
    db: AsyncSession,
) -> dict:
    kloklijst_result = await db.execute(
        select(Kloklijst.loonnummers, Kloklijst.naam)
        .where(
            Kloklijst.week_number == week,
            Kloklijst.agency == "otto",
            Kloklijst.naam.isnot(None),
        )
        .distinct()
    )
    factuur_result = await db.execute(
        select(InvoiceLine.sap_id, InvoiceLine.naam)
        .where(InvoiceLine.week_number == week)
        .distinct()
    )

    otto_people_by_name: dict[str, OttoPerson] = {}
    for loonnummer, naam in kloklijst_result:
        if not naam:
            continue
        kloklijst_name = str(naam).strip()
        if not kloklijst_name:
            continue
        normalized_name = _normalize_name(kloklijst_name)
        loonnummer_id = _to_id(loonnummer)
        existing = otto_people_by_name.get(normalized_name)
        if existing is None:
            otto_people_by_name[normalized_name] = OttoPerson(
                normalized_name=normalized_name,
                kloklijst_name=kloklijst_name,
                kloklijst_loonnummer=loonnummer_id,
            )
            continue
        if existing.kloklijst_loonnummer is None and loonnummer_id is not None:
            existing.kloklijst_loonnummer = loonnummer_id

    factuur_people_by_name: dict[str, FactuurPerson] = {}
    factuur_by_sap: dict[str, FactuurPerson] = {}
    for sap_id_raw, naam in factuur_result:
        sap_id = _to_id(sap_id_raw)
        factuur_name = str(naam).strip() if naam else ""
        if not sap_id or not factuur_name:
            continue
        normalized_name = _normalize_name(factuur_name)
        person = FactuurPerson(
            normalized_name=normalized_name,
            factuur_name=factuur_name,
            sap_id=sap_id,
        )
        factuur_by_sap[sap_id] = person
        factuur_people_by_name.setdefault(normalized_name, person)

    candidates: list[dict[str, object]] = []
    for person in sorted(otto_people_by_name.values(), key=lambda p: p.kloklijst_name.lower()):
        direct_id_match = (
            person.kloklijst_loonnummer is not None
            and person.kloklijst_loonnummer in factuur_by_sap
        )
        name_match_person = factuur_people_by_name.get(person.normalized_name)
        name_match_sap = name_match_person.sap_id if name_match_person else None
        resolved_sap: str | None = None
        resolved_factuur_name: str | None = None
        match_type: str

        if direct_id_match:
            resolved_sap = person.kloklijst_loonnummer
            resolved_factuur_name = factuur_by_sap[resolved_sap].factuur_name
            if name_match_sap and name_match_sap != resolved_sap:
                match_type = "id_name_conflict"
            else:
                match_type = "id_match"
        elif name_match_sap:
            resolved_sap = name_match_sap
            resolved_factuur_name = name_match_person.factuur_name
            match_type = "name_only"
        else:
            match_type = "missing"

        resolved = resolved_sap is not None
        candidates.append(
            {
                "week": week,
                "provider": "otto",
                "kloklijst_loonnummer": person.kloklijst_loonnummer,
                "kloklijst_name": person.kloklijst_name,
                "factuur_sap_id": resolved_sap,
                "factuur_name": resolved_factuur_name,
                "match_type": match_type,
                "resolved": resolved,
                "persistable": resolved and match_type in {"id_match", "name_only"},
            }
        )

    # Check internal uniqueness of persistable mappings.
    seen_by_loon: dict[str, str] = {}
    seen_by_sap: dict[str, str] = {}
    uniqueness_conflicts: list[dict[str, str]] = []
    for row in candidates:
        if not bool(row["persistable"]):
            continue
        loonnummer = _to_id(row["kloklijst_loonnummer"])
        sap_id = _to_id(row["factuur_sap_id"])
        if not loonnummer or not sap_id:
            row["persistable"] = False
            continue
        prev_sap = seen_by_loon.get(loonnummer)
        if prev_sap and prev_sap != sap_id:
            row["persistable"] = False
            uniqueness_conflicts.append(
                {
                    "type": "loonnummer_to_multiple_sap",
                    "loonnummer": loonnummer,
                    "sapA": prev_sap,
                    "sapB": sap_id,
                }
            )
        else:
            seen_by_loon[loonnummer] = sap_id
        prev_loon = seen_by_sap.get(sap_id)
        if prev_loon and prev_loon != loonnummer:
            row["persistable"] = False
            uniqueness_conflicts.append(
                {
                    "type": "sap_to_multiple_loonnummers",
                    "sap_id": sap_id,
                    "loonnummerA": prev_loon,
                    "loonnummerB": loonnummer,
                }
            )
        else:
            seen_by_sap[sap_id] = loonnummer

    total_people = len(candidates)
    persistable_count = sum(1 for row in candidates if bool(row["persistable"]))
    unresolved_count = total_people - persistable_count
    coverage_100 = unresolved_count == 0 and len(uniqueness_conflicts) == 0

    stats = {
        "week": week,
        "provider": "otto",
        "totalOttoPeople": total_people,
        "persistableMappings": persistable_count,
        "unresolvedPeople": unresolved_count,
        "coverage100": coverage_100,
        "uniquenessConflictCount": len(uniqueness_conflicts),
        "countByMatchType": {
            "id_match": sum(1 for row in candidates if row["match_type"] == "id_match"),
            "name_only": sum(1 for row in candidates if row["match_type"] == "name_only"),
            "id_name_conflict": sum(
                1 for row in candidates if row["match_type"] == "id_name_conflict"
            ),
            "missing": sum(1 for row in candidates if row["match_type"] == "missing"),
        },
    }
    return {
        "stats": stats,
        "candidates": candidates,
        "uniquenessConflicts": uniqueness_conflicts,
    }


def write_otto_mapping_csv(
    candidates: list[dict[str, object]],
    csv_path: Path = DEFAULT_BACKUP_CSV_PATH,
) -> str:
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_HEADERS)
        writer.writeheader()
        for row in candidates:
            writer.writerow(
                {
                    "week": row.get("week"),
                    "provider": row.get("provider"),
                    "kloklijst_loonnummer": row.get("kloklijst_loonnummer") or "",
                    "kloklijst_name": row.get("kloklijst_name") or "",
                    "factuur_sap_id": row.get("factuur_sap_id") or "",
                    "factuur_name": row.get("factuur_name") or "",
                    "match_type": row.get("match_type") or "",
                    "resolved": "true" if bool(row.get("resolved")) else "false",
                    "persistable": "true" if bool(row.get("persistable")) else "false",
                }
            )
    return str(csv_path)


async def persist_otto_mapping_candidates(
    week: int,
    candidates: list[dict[str, object]],
    db: AsyncSession,
) -> dict:
    persistable_rows = [
        row
        for row in candidates
        if bool(row.get("persistable"))
        and _to_id(row.get("kloklijst_loonnummer")) is not None
        and _to_id(row.get("factuur_sap_id")) is not None
    ]

    await db.execute(delete(OttoIdentifierMapping).where(OttoIdentifierMapping.provider == "otto"))
    for row in persistable_rows:
        db.add(
            OttoIdentifierMapping(
                provider="otto",
                kloklijst_loonnummer=str(row["kloklijst_loonnummer"]),
                sap_id=str(row["factuur_sap_id"]),
                kloklijst_name=str(row.get("kloklijst_name") or ""),
                factuur_name=str(row.get("factuur_name") or ""),
                match_type=str(row.get("match_type") or ""),
                verified=True,
                source_week=week,
            )
        )
    await db.commit()
    return {"insertedMappings": len(persistable_rows)}


async def load_verified_otto_mapping(
    db: AsyncSession,
) -> tuple[dict[str, str], set[str]]:
    try:
        result = await db.execute(
            select(OttoIdentifierMapping.kloklijst_loonnummer, OttoIdentifierMapping.sap_id).where(
                OttoIdentifierMapping.provider == "otto",
                OttoIdentifierMapping.verified.is_(True),
            )
        )
    except ProgrammingError as e:
        await db.rollback()
        error_text = str(e).lower()
        if "otto_identifier_mapping" in error_text and "does not exist" in error_text:
            return {}, set()
        raise

    loonnummer_to_sap: dict[str, str] = {}
    sap_ids: set[str] = set()
    for loonnummer, sap_id in result:
        loon_id = _to_id(loonnummer)
        sap = _to_id(sap_id)
        if not loon_id or not sap:
            continue
        loonnummer_to_sap[loon_id] = sap
        sap_ids.add(sap)
    return loonnummer_to_sap, sap_ids

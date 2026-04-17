from __future__ import annotations

import csv
from collections import Counter
from pathlib import Path
import re

from sqlalchemy import select
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.ext.asyncio import AsyncSession

from models import InvoiceLine, OttoIdentifierMapping, PersonWagegroup, PersonWagegroupRate
from otto_identifier_mapping import build_otto_mapping_candidates


def normalize_person_name(name: str) -> str:
    return " ".join(sorted(name.lower().replace("-", " ").split()))


def fallback_person_number(name: str) -> str:
    return f"name:{normalize_person_name(name)}"


def extract_embedded_number_and_clean_name(name: str) -> tuple[str, str | None]:
    """
    Extract a standalone numeric token from names like:
    'Ailoaei 16444322 Emanuel-Florin' -> ('Ailoaei Emanuel-Florin', '16444322')
    """
    text = " ".join(name.strip().split())
    match = re.search(r"\b(\d{6,12})\b", text)
    if not match:
        return text, None
    number = match.group(1)
    cleaned = re.sub(rf"\b{re.escape(number)}\b", " ", text)
    cleaned = " ".join(cleaned.split())
    return cleaned, number


def _to_id(value: object | None) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text if text else None


def _pick_wagegroup(values: list[str]) -> tuple[str, bool]:
    """
    Returns (selected_wagegroup, had_conflict).
    Uses most frequent non-empty wagegroup in invoice rows.
    """
    cleaned = [v.strip() for v in values if v and v.strip()]
    if not cleaned:
        return "", False
    counter = Counter(cleaned)
    top_wagegroup, _ = counter.most_common(1)[0]
    return top_wagegroup, len(counter) > 1


def _extract_tarief_letter(value: str) -> str:
    text = value.strip().upper()
    if not text:
        return ""
    m = re.search(r"\bFASE\s+([A-Z])\b", text)
    if m:
        return m.group(1)
    m = re.search(r"([A-Z])$", text)
    if m:
        return m.group(1)
    return text


async def load_otto_identity_context(db: AsyncSession) -> dict[str, object]:
    try:
        mapping_result = await db.execute(
            select(
                OttoIdentifierMapping.kloklijst_loonnummer,
                OttoIdentifierMapping.sap_id,
                OttoIdentifierMapping.kloklijst_name,
                OttoIdentifierMapping.factuur_name,
            ).where(
                OttoIdentifierMapping.provider == "otto",
                OttoIdentifierMapping.verified.is_(True),
            )
        )
    except ProgrammingError as e:
        await db.rollback()
        error_text = str(e).lower()
        if "otto_identifier_mapping" in error_text and "does not exist" in error_text:
            mapping_rows = []
        else:
            raise
    else:
        mapping_rows = list(mapping_result)

    by_loon: dict[str, tuple[str, str, str]] = {}
    by_sap: dict[str, tuple[str, str, str]] = {}
    by_name: dict[str, tuple[str, str, str]] = {}
    for loon, sap, klok_name, factuur_name in mapping_rows:
        loon_id = _to_id(loon)
        sap_id = _to_id(sap)
        if not loon_id or not sap_id:
            continue
        klok = str(klok_name or "").strip()
        fact = str(factuur_name or "").strip()
        by_loon[loon_id] = (sap_id, loon_id, klok)
        by_sap[sap_id] = (sap_id, loon_id, fact)
        if klok:
            by_name.setdefault(normalize_person_name(klok), (sap_id, loon_id, klok))
        if fact:
            by_name.setdefault(normalize_person_name(fact), (sap_id, loon_id, fact))

    known_rows = await list_person_wagegroups(db, provider="otto")
    known_canonical_by_name = {
        normalize_person_name(row.name): row
        for row in known_rows
        if not row.person_number.startswith("name:")
    }

    return {
        "by_loon": by_loon,
        "by_sap": by_sap,
        "by_name": by_name,
        "known_canonical_by_name": known_canonical_by_name,
    }


async def resolve_wagegroup_identity(
    db: AsyncSession,
    *,
    provider: str,
    raw_name: str,
    supplied_person_number: str | None = None,
    otto_context: dict[str, object] | None = None,
) -> dict[str, object]:
    provider_normalized = provider.strip().lower()
    cleaned_name, embedded_number = extract_embedded_number_and_clean_name(raw_name)
    supplied_id = _to_id(supplied_person_number)
    candidate_number = supplied_id or embedded_number

    # Default identity: fallback by cleaned name.
    resolved = {
        "name": cleaned_name or raw_name.strip(),
        "person_number": fallback_person_number(cleaned_name or raw_name),
        "kloklijst_loonnummer": None,
        "verified": False,
        "match_method": "name_fallback",
    }

    if provider_normalized != "otto":
        return resolved

    context = otto_context or await load_otto_identity_context(db)
    by_loon: dict[str, tuple[str, str, str]] = context["by_loon"]  # type: ignore[assignment]
    by_sap: dict[str, tuple[str, str, str]] = context["by_sap"]  # type: ignore[assignment]
    by_name: dict[str, tuple[str, str, str]] = context["by_name"]  # type: ignore[assignment]

    # 1) Number-based mapping from supplied personNumber or embedded name number.
    if candidate_number:
        if candidate_number in by_sap:
            sap_id, loon_id, _ = by_sap[candidate_number]
            return {
                "name": cleaned_name or raw_name.strip(),
                "person_number": sap_id,
                "kloklijst_loonnummer": loon_id,
                "verified": True,
                "match_method": "id_direct_sap",
            }
        if candidate_number in by_loon:
            sap_id, loon_id, _ = by_loon[candidate_number]
            return {
                "name": cleaned_name or raw_name.strip(),
                "person_number": sap_id,
                "kloklijst_loonnummer": loon_id,
                "verified": True,
                "match_method": "id_mapped_loonnummer",
            }

    # 2) Name-based mapping from verified comparison tool mappings.
    normalized_name = normalize_person_name(cleaned_name or raw_name)
    mapped = by_name.get(normalized_name)
    if mapped:
        sap_id, loon_id, _ = mapped
        return {
            "name": cleaned_name or raw_name.strip(),
            "person_number": sap_id,
            "kloklijst_loonnummer": loon_id,
            "verified": True,
            "match_method": "name_mapped_verified",
        }

    # 3) If an existing canonical record already exists for this person name, reuse it.
    known_canonical_by_name: dict[str, PersonWagegroup] = context["known_canonical_by_name"]  # type: ignore[assignment]
    known = known_canonical_by_name.get(normalized_name)
    if known:
        return {
            "name": cleaned_name or raw_name.strip(),
            "person_number": known.person_number,
            "kloklijst_loonnummer": known.kloklijst_loonnummer,
            "verified": bool(known.verified),
            "match_method": "name_existing_canonical",
        }

    # Keep embedded number only as audit hint if unresolved.
    resolved["kloklijst_loonnummer"] = embedded_number
    resolved["match_method"] = "name_fallback_with_embedded_number" if embedded_number else "name_fallback"
    return resolved


async def list_person_wagegroups(
    db: AsyncSession,
    provider: str | None = None,
) -> list[PersonWagegroup]:
    stmt = select(PersonWagegroup)
    if provider:
        stmt = stmt.where(PersonWagegroup.provider == provider)
    result = await db.execute(stmt.order_by(PersonWagegroup.name.asc()))
    return list(result.scalars().all())


async def upsert_person_wagegroup(
    db: AsyncSession,
    *,
    provider: str,
    person_number: str,
    name: str,
    wagegroup: str,
    kloklijst_loonnummer: str | None = None,
    verified: bool = True,
    source_week: int | None = None,
) -> str:
    stmt = select(PersonWagegroup).where(
        PersonWagegroup.provider == provider,
        PersonWagegroup.person_number == person_number,
    )
    result = await db.execute(stmt)
    existing = result.scalar_one_or_none()
    if existing:
        existing.name = name
        existing.wagegroup = wagegroup
        existing.kloklijst_loonnummer = kloklijst_loonnummer
        existing.verified = verified
        existing.source_week = source_week
        await db.commit()
        return "updated"

    db.add(
        PersonWagegroup(
            provider=provider,
            person_number=person_number,
            name=name,
            wagegroup=wagegroup,
            kloklijst_loonnummer=kloklijst_loonnummer,
            verified=verified,
            source_week=source_week,
        )
    )
    await db.commit()
    return "inserted"


async def bulk_upsert_person_wagegroups(
    db: AsyncSession,
    rows: list[dict[str, object]],
) -> dict[str, int]:
    inserted = 0
    updated = 0
    for row in rows:
        action = await upsert_person_wagegroup(
            db,
            provider=str(row["provider"]),
            person_number=str(row["person_number"]),
            name=str(row["name"]),
            wagegroup=str(row["wagegroup"]),
            kloklijst_loonnummer=_to_id(row.get("kloklijst_loonnummer")),
            verified=bool(row.get("verified", True)),
            source_week=row.get("source_week") if isinstance(row.get("source_week"), int) else None,
        )
        if action == "inserted":
            inserted += 1
        else:
            updated += 1
    return {"inserted": inserted, "updated": updated}


async def backfill_wagegroups_from_csv(
    week: int,
    db: AsyncSession,
    csv_path: Path,
    provider: str = "otto",
) -> dict:
    if not csv_path.exists():
        raise ValueError(f"CSV-bestand niet gevonden: {csv_path}")

    candidates_data = await build_otto_mapping_candidates(week=week, db=db)
    candidates = candidates_data["candidates"]
    by_name: dict[str, dict[str, object]] = {}
    for c in candidates:
        normalized = normalize_person_name(str(c["kloklijst_name"]))
        by_name[normalized] = c

    rows: list[dict[str, object]] = []
    mapped = 0
    fallback = 0

    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            name = str(row.get("name", "")).strip()
            wagegroup = str(row.get("wagegroup", "")).strip()
            if not name or not wagegroup:
                continue
            normalized = normalize_person_name(name)
            candidate = by_name.get(normalized)
            person_number = None
            kloklijst_loonnummer = None
            verified = False
            if candidate and candidate.get("factuur_sap_id"):
                person_number = str(candidate["factuur_sap_id"])
                kloklijst_loonnummer = _to_id(candidate.get("kloklijst_loonnummer"))
                verified = bool(candidate.get("persistable"))
                mapped += 1
            else:
                person_number = fallback_person_number(name)
                fallback += 1
            rows.append(
                {
                    "provider": provider,
                    "person_number": person_number,
                    "name": name,
                    "wagegroup": wagegroup,
                    "kloklijst_loonnummer": kloklijst_loonnummer,
                    "verified": verified,
                    "source_week": week,
                }
            )

    result = await bulk_upsert_person_wagegroups(db, rows)
    return {
        "status": "ok",
        "week": week,
        "provider": provider,
        "csvPath": str(csv_path),
        "mappedByIdentifier": mapped,
        "fallbackByName": fallback,
        **result,
        "candidateCoverage": candidates_data["stats"],
    }


async def analyze_otto_wagegroups(
    week: int,
    db: AsyncSession,
    *,
    include_mismatches: bool = True,
    max_items: int = 500,
) -> dict:
    invoice_result = await db.execute(
        select(InvoiceLine.sap_id, InvoiceLine.naam, InvoiceLine.fase_tarief).where(
            InvoiceLine.week_number == week
        )
    )
    invoice_rows = list(invoice_result)
    by_sap: dict[str, dict[str, object]] = {}
    for sap_id_raw, naam_raw, fase_raw in invoice_rows:
        sap_id = _to_id(sap_id_raw)
        if not sap_id:
            continue
        entry = by_sap.setdefault(
            sap_id,
            {
                "name": str(naam_raw or "").strip(),
                "fase_values": [],
            },
        )
        entry["fase_values"].append(str(fase_raw or "").strip())
        if not entry["name"] and naam_raw:
            entry["name"] = str(naam_raw).strip()

    known_rows = await list_person_wagegroups(db, provider="otto")
    known_by_person = {row.person_number: row for row in known_rows}
    fallback_by_name = {
        normalize_person_name(row.name): row
        for row in known_rows
        if row.person_number.startswith("name:")
    }
    rate_result = await db.execute(
        select(PersonWagegroupRate).where(
            PersonWagegroupRate.provider == "otto",
            PersonWagegroupRate.rate_key == "100",
        )
    )
    person_rate_rows = list(rate_result.scalars().all())
    person_tarief_by_person = {
        row.person_number: (row.tarief or "").strip().upper()
        for row in person_rate_rows
        if row.person_number and row.tarief
    }
    person_tarief_by_name = {
        row.normalized_name: (row.tarief or "").strip().upper()
        for row in person_rate_rows
        if row.normalized_name and row.tarief
    }
    # Also build a SAP->wagegroup map from rates so we can treat person_wagegroup_rates
    # as a first-class reference source when person_wagegroups lacks an entry.
    rate_wagegroup_by_person: dict[str, str] = {}
    rate_wagegroup_by_name: dict[str, str] = {}
    for row in person_rate_rows:
        schaal = (row.schaal or "").strip()
        tarief = (row.tarief or "").strip()
        if not schaal and not tarief:
            continue
        wg = f"{schaal} / Fase {tarief}".strip() if tarief else schaal
        if row.person_number:
            rate_wagegroup_by_person.setdefault(row.person_number, wg)
        if row.normalized_name:
            rate_wagegroup_by_name.setdefault(row.normalized_name, wg)

    wagegroup_matches = 0
    wagegroup_mismatches = 0
    missing_known_wagegroup = 0
    matched_by_person_number = 0
    matched_by_name_fallback = 0
    invoice_wagegroup_conflicts = 0
    mismatches: list[dict[str, str]] = []

    for sap_id, info in by_sap.items():
        invoice_name = str(info["name"])
        invoice_wagegroup, had_conflict = _pick_wagegroup(info["fase_values"])
        if had_conflict:
            invoice_wagegroup_conflicts += 1

        normalized_invoice = normalize_person_name(invoice_name)
        known = known_by_person.get(sap_id)
        match_method = "person_number"
        if known:
            matched_by_person_number += 1
        else:
            known = fallback_by_name.get(normalized_invoice)
            match_method = "name_fallback"
            if known:
                matched_by_name_fallback += 1

        known_tarief = person_tarief_by_person.get(sap_id) or person_tarief_by_name.get(normalized_invoice)
        known_wagegroup = (known.wagegroup.strip() if known else "") or (
            rate_wagegroup_by_person.get(sap_id) or rate_wagegroup_by_name.get(normalized_invoice) or ""
        )
        has_rate_reference = bool(known_tarief) or bool(
            rate_wagegroup_by_person.get(sap_id) or rate_wagegroup_by_name.get(normalized_invoice)
        )

        if not known and not has_rate_reference:
            missing_known_wagegroup += 1
            if include_mismatches and len(mismatches) < max_items:
                mismatches.append(
                    {
                        "sapId": sap_id,
                        "name": invoice_name,
                        "invoiceWagegroup": invoice_wagegroup,
                        "knownWagegroup": "",
                        "status": "missing_known_wagegroup",
                        "matchMethod": "",
                    }
                )
            continue

        if not known_tarief:
            known_tarief = _extract_tarief_letter(known_wagegroup)
        invoice_tarief = _extract_tarief_letter(invoice_wagegroup)
        # If the invoice has no wagegroup value, skip comparison: there is nothing
        # to compare against (reported elsewhere as missing invoice data).
        if not invoice_tarief:
            continue
        if known_tarief == invoice_tarief:
            wagegroup_matches += 1
            continue
        wagegroup_mismatches += 1
        if include_mismatches and len(mismatches) < max_items:
            mismatches.append(
                {
                    "sapId": sap_id,
                    "name": invoice_name,
                    "invoiceWagegroup": invoice_tarief or invoice_wagegroup,
                    "knownWagegroup": known_tarief or known_wagegroup,
                    "status": "mismatch",
                    "matchMethod": match_method or "rate_reference",
                }
            )

    return {
        "status": "ok",
        "week": week,
        "provider": "otto",
        "invoicePeople": len(by_sap),
        "knownWagegroups": len(known_rows),
        "matchedByPersonNumber": matched_by_person_number,
        "matchedByNameFallback": matched_by_name_fallback,
        "wagegroupMatches": wagegroup_matches,
        "wagegroupMismatches": wagegroup_mismatches,
        "missingKnownWagegroup": missing_known_wagegroup,
        "invoiceWagegroupConflicts": invoice_wagegroup_conflicts,
        "coverage100": missing_known_wagegroup == 0 and wagegroup_mismatches == 0,
        "mismatches": mismatches if include_mismatches else [],
    }


async def verify_otto_wagegroup_coverage(
    week: int,
    db: AsyncSession,
) -> dict:
    mapping = await build_otto_mapping_candidates(week=week, db=db)
    mapping_stats = mapping["stats"]

    invoice_result = await db.execute(
        select(InvoiceLine.sap_id).where(InvoiceLine.week_number == week).distinct()
    )
    invoice_sap_ids = {_to_id(sap_id) for (sap_id,) in invoice_result}
    invoice_sap_ids.discard(None)

    known_rows = await list_person_wagegroups(db, provider="otto")
    known_person_numbers = {
        row.person_number for row in known_rows if not row.person_number.startswith("name:")
    }
    fallback_rows = [row for row in known_rows if row.person_number.startswith("name:")]

    missing_canonical = sorted(invoice_sap_ids - known_person_numbers)
    db_coverage_100 = len(missing_canonical) == 0 and len(invoice_sap_ids) > 0
    uniqueness_ok = mapping_stats.get("uniquenessConflictCount", 0) == 0
    mapping_coverage_100 = bool(mapping_stats.get("coverage100", False))
    ready_for_runtime = db_coverage_100 and uniqueness_ok and mapping_coverage_100

    return {
        "status": "ok",
        "week": week,
        "provider": "otto",
        "invoicePeopleBySap": len(invoice_sap_ids),
        "knownCanonicalPeople": len(known_person_numbers),
        "knownFallbackRecords": len(fallback_rows),
        "missingCanonicalPersonNumbers": missing_canonical,
        "dbCoverage100": db_coverage_100,
        "mappingCoverage100": mapping_coverage_100,
        "uniquenessOk": uniqueness_ok,
        "readyForRuntimeSwitch": ready_for_runtime,
        "mappingStats": mapping_stats,
    }

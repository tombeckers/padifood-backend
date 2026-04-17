"""
validation_hours.py

Compares hours per employee per wage category between:
  - invoice_lines table (from "Padifood specificatie - Export Factuur")
      → sum of totaal_uren grouped by naam + code_toeslag
  - kloklijst table (agency='otto', from "Kloklijst Padifood Otto Workforce")
      → sum of each hour-type column grouped by employee naam

Names are normalized (lowercase, words sorted) before matching, because the two
sources use different name orderings (firstname-lastname vs lastname-firstname).
"""

import argparse
import asyncio
import csv
import os
import re
from collections import defaultdict
from difflib import SequenceMatcher

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from config import Settings
from models import InvoiceLine, Kloklijst, PersonWagegroupRate
from otto_identifier_mapping import load_verified_otto_mapping
from validation_wagegroups import analyze_otto_wagegroups
from wagegroup_rates import analyze_otto_rate_mismatches
from wagegroup_rates import analyze_wagegroup_differences_by_rate

_EMBEDDED_NUM_RE = re.compile(r"\b\d{6,12}\b")


def _strip_embedded_numbers(name: str) -> str:
    """Remove embedded 6-12 digit tokens (e.g. SAP numbers in kloklijst names)."""
    if not name:
        return name
    cleaned = _EMBEDDED_NUM_RE.sub(" ", name)
    return " ".join(cleaned.split())

# Maps the original kloklijst column names (used in output/comparison) to
# the corresponding ORM field names on the Kloklijst model.
KLOKLIJST_COL_TO_FIELD: dict[str, str] = {
    "Norm uren Dag": "norm_uren_dag",
    "T133 Dag": "t133_dag",
    "T135 Dag": "t135_dag",
    "T200 Dag": "t200_dag",
    "OW140 Week": "ow140_week",
    "OW180 Dag": "ow180_dag",
    "OW200 Dag": "ow200_dag",
}

# Premium (non-norm) fields used to derive norm hours from effectieve_uren_dag.
_PREMIUM_FIELDS = ["t133_dag", "t135_dag", "t200_dag", "ow140_week", "ow180_dag", "ow200_dag"]


def _kloklijst_norm_uren(row: "Kloklijst") -> float | None:
    """
    Derive norm hours as effectieve_uren_dag minus all premium hours.

    Kelio double-counts overtime: OW hours appear both in norm_uren_dag and
    in their own OW column. The invoice derives norm as effectieve - premiums,
    so we do the same for an apples-to-apples comparison.

    Legacy fallback: if effectieve_uren_dag is absent, still subtract premium
    hours from norm_uren_dag when possible so Kelio OW double-counting is
    corrected in both paths.
    """
    if row.effectieve_uren_dag is not None:
        premium = sum(getattr(row, f) or 0.0 for f in _PREMIUM_FIELDS)
        return max(row.effectieve_uren_dag - premium, 0.0)
    if row.norm_uren_dag is not None:
        premium = sum(getattr(row, f) or 0.0 for f in _PREMIUM_FIELDS)
        return max(row.norm_uren_dag - premium, 0.0)
    return None


FUZZY_MATCH_THRESHOLD = 90
settings = Settings()


def normalize_name(name: str) -> str:
    """Lowercase and sort words so 'Dawid Kutermak' == 'Kutermak Dawid'."""
    return " ".join(sorted(name.lower().replace("-", " ").split()))


def _name_compare_key(name: str) -> str:
    return f"name:{normalize_name(name)}"


def _id_text(value: object | None) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text if text else None


def _invoice_compare_key(
    *,
    agency: str,
    sap_id: object | None,
    name: str | None,
    mapped_sap_ids: set[str],
) -> str | None:
    sap = _id_text(sap_id)
    if agency == "otto" and sap and sap in mapped_sap_ids:
        return f"id:{sap}"
    if not name:
        return None
    return _name_compare_key(name)


def _kloklijst_compare_key(
    *,
    agency: str,
    loonnummers: object | None,
    name: str | None,
    loonnummer_to_sap: dict[str, str],
) -> str | None:
    loonnummer = _id_text(loonnummers)
    if agency == "otto" and loonnummer and loonnummer in loonnummer_to_sap:
        return f"id:{loonnummer_to_sap[loonnummer]}"
    if not name:
        return None
    cleaned = _strip_embedded_numbers(name) if agency == "otto" else name
    return _name_compare_key(cleaned or name)


def _display_name_for_key(key: str, display_names: dict[str, str]) -> str:
    display = display_names.get(key)
    if display:
        return display
    if key.startswith("name:"):
        return key.split(":", 1)[1]
    if key.startswith("id:"):
        return key.split(":", 1)[1]
    return key


def _pair_key(kloklijst_name: str, factuur_name: str) -> tuple[str, str]:
    return (_name_compare_key(kloklijst_name), _name_compare_key(factuur_name))


def _build_alias_map(
    all_names: set[str], confirmed_same_pairs: list[tuple[str, str]]
) -> dict[str, str]:
    parent = {name: name for name in all_names}

    def find(node: str) -> str:
        if parent[node] != node:
            parent[node] = find(parent[node])
        return parent[node]

    def union(left: str, right: str) -> None:
        root_left = find(left)
        root_right = find(right)
        if root_left == root_right:
            return
        canonical = min(root_left, root_right)
        other = root_right if canonical == root_left else root_left
        parent[other] = canonical

    for left, right in confirmed_same_pairs:
        left_key = _name_compare_key(left)
        right_key = _name_compare_key(right)
        if left_key not in parent or right_key not in parent:
            continue
        union(left_key, right_key)

    return {name: find(name) for name in all_names}


def _merge_week_hours_by_alias(
    data: dict[str, dict[str, float]], alias_map: dict[str, str]
) -> dict[str, dict[str, float]]:
    merged: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    for name, categories in data.items():
        canonical = alias_map.get(name, name)
        for category, hours in categories.items():
            merged[canonical][category] += hours
    return {name: dict(categories) for name, categories in merged.items()}


def _merge_daily_hours_by_alias(
    data: dict[str, dict[str, dict[str, float]]], alias_map: dict[str, str]
) -> dict[str, dict[str, dict[str, float]]]:
    merged: dict = defaultdict(lambda: defaultdict(lambda: defaultdict(float)))
    for name, by_date in data.items():
        canonical = alias_map.get(name, name)
        for date_key, categories in by_date.items():
            for category, hours in categories.items():
                merged[canonical][date_key][category] += hours
    return merged


def _fuzzy_score(left: str, right: str) -> int:
    return round(100 * SequenceMatcher(None, left, right).ratio())


_INITIAL_TOKEN_RE = re.compile(r"^[A-Za-z]\.?$|^[A-Z]{2,3}$")


def _is_initials_name(display_name: str) -> bool:
    """Return True if name is in initials + lastname format.
    Handles 'A Lademann', 'D.D. Baciu', 'A.B. Smith', etc.
    """
    tokens = display_name.strip().split()
    return len(tokens) >= 2 and all(
        _INITIAL_TOKEN_RE.match(t) or "." in t for t in tokens[:-1]
    )


def _find_similar_name_pairs(
    factuur_names: set[str],
    kloklijst_names: set[str],
    factuur_display_map: dict[str, str],
    kloklijst_display_map: dict[str, str],
    confirmed_diff_pairs: set[tuple[str, str]],
) -> list[tuple[str, str]]:
    similar_pairs: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()

    kloklijst_only = sorted(
        name for name in (kloklijst_names - factuur_names) if name.startswith("name:")
    )
    factuur_only = sorted(
        name for name in (factuur_names - kloklijst_names) if name.startswith("name:")
    )

    for kloklijst_name in kloklijst_only:
        for factuur_name in factuur_only:
            key = (kloklijst_name, factuur_name)
            if key in confirmed_diff_pairs:
                continue
            klok_raw = kloklijst_name.split(":", 1)[1]
            factuur_raw = factuur_name.split(":", 1)[1]
            if _fuzzy_score(klok_raw, factuur_raw) <= FUZZY_MATCH_THRESHOLD:
                continue
            display_pair = (
                kloklijst_display_map.get(kloklijst_name, kloklijst_name),
                factuur_display_map.get(factuur_name, factuur_name),
            )
            if display_pair in seen:
                continue
            seen.add(display_pair)
            similar_pairs.append(display_pair)

    # Second pass: initials-based matching for Flex invoice names.
    # e.g. factuur "D.D. Baciu" → last name "baciu" → matches kloklijst "Baciu Dumitru".
    for factuur_key in factuur_only:
        display_name = factuur_display_map.get(factuur_key, "")
        if not display_name or not _is_initials_name(display_name):
            continue
        last_name = display_name.strip().split()[-1].lower()
        for kloklijst_key in kloklijst_only:
            if (kloklijst_key, factuur_key) in confirmed_diff_pairs:
                continue
            klok_display = kloklijst_display_map.get(kloklijst_key, "")
            if not klok_display:
                continue
            if last_name not in [w.lower() for w in klok_display.split()]:
                continue
            display_pair = (
                kloklijst_display_map.get(kloklijst_key, kloklijst_key),
                factuur_display_map.get(factuur_key, factuur_key),
            )
            if display_pair in seen:
                continue
            seen.add(display_pair)
            similar_pairs.append(display_pair)

    similar_pairs.sort(key=lambda pair: (pair[0].lower(), pair[1].lower()))
    return similar_pairs


# ---------------------------------------------------------------------------
# DB-backed loaders
# ---------------------------------------------------------------------------


async def _load_factuur_hours_db(
    week: int,
    db: AsyncSession,
    agency: str,
    mapped_sap_ids: set[str],
) -> dict[str, dict[str, float]]:
    """Returns {compare_key: {code_toeslag: total_uren}} from invoice_lines."""
    result = await db.execute(
        select(InvoiceLine).where(
            InvoiceLine.week_number == week,
            InvoiceLine.agency == agency,
        )
    )
    totals: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    for row in result.scalars().all():
        if not row.naam or not row.code_toeslag:
            continue
        compare_key = _invoice_compare_key(
            agency=agency,
            sap_id=row.sap_id,
            name=row.naam,
            mapped_sap_ids=mapped_sap_ids,
        )
        if not compare_key:
            continue
        totals[compare_key][row.code_toeslag] += row.totaal_uren
    return {k: dict(v) for k, v in totals.items()}


async def _load_kloklijst_hours_db(
    week: int,
    db: AsyncSession,
    agency: str,
    loonnummer_to_sap: dict[str, str],
) -> dict[str, dict[str, float]]:
    """Returns {compare_key: {col_name: total_uren}} from kloklijst for an agency.

    Kelio adjustment: Norm uren Dag = Σ effectieve_uren_dag − Σ all premium hours.
    Because premium hours (especially OW140_week) can appear on rows where
    effectieve_uren_dag is NULL, we aggregate per person first then compute Norm
    once, rather than subtracting per-row.
    """
    result = await db.execute(
        select(Kloklijst).where(
            Kloklijst.week_number == week,
            Kloklijst.agency == agency,
            Kloklijst.datum.isnot(None),
        )
    )
    premium_totals: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    effectieve_total: dict[str, float] = defaultdict(float)
    norm_raw_total: dict[str, float] = defaultdict(float)
    keys_seen: set[str] = set()
    for row in result.scalars().all():
        if not row.naam:
            continue
        compare_key = _kloklijst_compare_key(
            agency=agency,
            loonnummers=row.loonnummers,
            name=row.naam,
            loonnummer_to_sap=loonnummer_to_sap,
        )
        if not compare_key:
            continue
        keys_seen.add(compare_key)
        if row.effectieve_uren_dag:
            effectieve_total[compare_key] += row.effectieve_uren_dag
        if row.norm_uren_dag:
            norm_raw_total[compare_key] += row.norm_uren_dag
        for col_name, field in KLOKLIJST_COL_TO_FIELD.items():
            if col_name == "Norm uren Dag":
                continue
            val = getattr(row, field, None)
            if val:
                premium_totals[compare_key][col_name] += val

    totals: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    for key in keys_seen:
        for col_name, val in premium_totals[key].items():
            if val:
                totals[key][col_name] += val
        premium_sum = sum(premium_totals[key].values())
        base = effectieve_total[key] if effectieve_total[key] > 0 else norm_raw_total[key]
        norm = max(base - premium_sum, 0.0) if base > 0 else 0.0
        if norm:
            totals[key]["Norm uren Dag"] += norm
    return {k: dict(v) for k, v in totals.items()}


async def _load_factuur_hours_by_date_db(
    week: int,
    db: AsyncSession,
    agency: str,
    mapped_sap_ids: set[str],
) -> dict[str, dict[str, dict[str, float]]]:
    """Returns {compare_key: {date: {code_toeslag: total_uren}}} from invoice_lines."""
    result = await db.execute(
        select(InvoiceLine).where(
            InvoiceLine.week_number == week,
            InvoiceLine.agency == agency,
        )
    )
    totals: dict = defaultdict(lambda: defaultdict(lambda: defaultdict(float)))
    for row in result.scalars().all():
        if not row.naam or not row.code_toeslag or row.datum is None:
            continue
        compare_key = _invoice_compare_key(
            agency=agency,
            sap_id=row.sap_id,
            name=row.naam,
            mapped_sap_ids=mapped_sap_ids,
        )
        if not compare_key:
            continue
        totals[compare_key][str(row.datum)][row.code_toeslag] += row.totaal_uren
    return totals


async def _load_kloklijst_hours_by_date_db(
    week: int,
    db: AsyncSession,
    agency: str,
    loonnummer_to_sap: dict[str, str],
) -> dict[str, dict[str, dict[str, float]]]:
    """Returns {compare_key: {date: {col_name: total_uren}}} from kloklijst for an agency.

    Kelio adjustment applied per (person, date): Norm = effectieve − Σ premiums.
    """
    result = await db.execute(
        select(Kloklijst).where(
            Kloklijst.week_number == week,
            Kloklijst.agency == agency,
            Kloklijst.datum.isnot(None),
        )
    )
    premium_by_date: dict = defaultdict(lambda: defaultdict(lambda: defaultdict(float)))
    effectieve_by_date: dict = defaultdict(lambda: defaultdict(float))
    norm_raw_by_date: dict = defaultdict(lambda: defaultdict(float))
    date_keys: set[tuple[str, str]] = set()
    for row in result.scalars().all():
        if not row.naam:
            continue
        compare_key = _kloklijst_compare_key(
            agency=agency,
            loonnummers=row.loonnummers,
            name=row.naam,
            loonnummer_to_sap=loonnummer_to_sap,
        )
        if not compare_key:
            continue
        date_key = str(row.datum)
        date_keys.add((compare_key, date_key))
        if row.effectieve_uren_dag:
            effectieve_by_date[compare_key][date_key] += row.effectieve_uren_dag
        if row.norm_uren_dag:
            norm_raw_by_date[compare_key][date_key] += row.norm_uren_dag
        for col_name, field in KLOKLIJST_COL_TO_FIELD.items():
            if col_name == "Norm uren Dag":
                continue
            val = getattr(row, field, None)
            if val:
                premium_by_date[compare_key][date_key][col_name] += val

    totals: dict = defaultdict(lambda: defaultdict(lambda: defaultdict(float)))
    for compare_key, date_key in date_keys:
        for col_name, val in premium_by_date[compare_key][date_key].items():
            if val:
                totals[compare_key][date_key][col_name] += val
        premium_sum = sum(premium_by_date[compare_key][date_key].values())
        base = effectieve_by_date[compare_key][date_key]
        if base <= 0:
            base = norm_raw_by_date[compare_key][date_key]
        norm = max(base - premium_sum, 0.0) if base > 0 else 0.0
        if norm:
            totals[compare_key][date_key]["Norm uren Dag"] += norm
    return totals


async def _build_name_display_maps_db(
    week: int,
    db: AsyncSession,
    agency: str,
    mapped_sap_ids: set[str],
    loonnummer_to_sap: dict[str, str],
) -> tuple[dict[str, str], dict[str, str]]:
    """Returns compare-key display maps for factuur and kloklijst."""
    factuur_result = await db.execute(
        select(InvoiceLine.sap_id, InvoiceLine.naam)
        .where(InvoiceLine.week_number == week)
        .distinct()
    )
    factuur_names: dict[str, str] = {}
    for sap_id, naam in factuur_result:
        if naam:
            compare_key = _invoice_compare_key(
                agency=agency,
                sap_id=sap_id,
                name=naam,
                mapped_sap_ids=mapped_sap_ids,
            )
            if compare_key:
                factuur_names.setdefault(compare_key, naam)

    kloklijst_result = await db.execute(
        select(Kloklijst.loonnummers, Kloklijst.naam)
        .where(
            Kloklijst.week_number == week,
            Kloklijst.agency == agency,
            Kloklijst.naam.isnot(None),
        )
        .distinct()
    )
    kloklijst_names: dict[str, str] = {}
    for loonnummers, naam in kloklijst_result:
        if naam:
            compare_key = _kloklijst_compare_key(
                agency=agency,
                loonnummers=loonnummers,
                name=naam,
                loonnummer_to_sap=loonnummer_to_sap,
            )
            if compare_key:
                kloklijst_names.setdefault(compare_key, naam)

    return factuur_names, kloklijst_names


def _apply_alias_to_display_map(
    display_map: dict[str, str], alias_map: dict[str, str]
) -> dict[str, str]:
    remapped: dict[str, str] = {}
    for original_name, display_name in display_map.items():
        canonical = alias_map.get(original_name, original_name)
        remapped.setdefault(canonical, display_name)
    return remapped


def build_rows(
    factuur, kloklijst, include_date=False, display_names: dict[str, str] | None = None
):
    """
    Combine factuur and kloklijst dicts into comparison rows.

    Without date: factuur/kloklijst are {name: {cat: hours}}
    With date:    factuur/kloklijst are {name: {date: {cat: hours}}}
    """
    rows = []
    counts = {"ok": 0, "verschil": 0, "only_factuur": 0, "only_kloklijst": 0}

    all_names = sorted(set(factuur) | set(kloklijst))
    for name in all_names:
        f_by_name = factuur.get(name, {})
        k_by_name = kloklijst.get(name, {})

        if include_date:
            all_dates = sorted(set(f_by_name) | set(k_by_name))
            iterations = [
                (date, f_by_name.get(date, {}), k_by_name.get(date, {}))
                for date in all_dates
            ]
        else:
            iterations = [(None, f_by_name, k_by_name)]

        for date, f_cats, k_cats in iterations:
            all_cats = sorted(set(f_cats) | set(k_cats))
            for cat in all_cats:
                f_uren = f_cats.get(cat)
                k_uren = k_cats.get(cat)

                shown_name = _display_name_for_key(name, display_names or {})
                base = {"Naam": shown_name}
                if include_date:
                    base["Datum"] = date
                base["Code toeslag"] = cat

                if f_uren is None:
                    rows.append(
                        {
                            **base,
                            "Factuur uren": "",
                            "Kloklijst uren": round(k_uren, 2),
                            "Verschil": "",
                            "Status": "ALLEEN IN KLOKLIJST",
                        }
                    )
                    counts["only_kloklijst"] += 1
                elif k_uren is None:
                    rows.append(
                        {
                            **base,
                            "Factuur uren": round(f_uren, 2),
                            "Kloklijst uren": "",
                            "Verschil": "",
                            "Status": "ALLEEN IN FACTUUR",
                        }
                    )
                    counts["only_factuur"] += 1
                else:
                    diff = round(f_uren - k_uren, 2)
                    status = "OK" if diff == 0 else "VERSCHIL"
                    counts["verschil" if diff != 0 else "ok"] += 1
                    rows.append(
                        {
                            **base,
                            "Factuur uren": round(f_uren, 2),
                            "Kloklijst uren": round(k_uren, 2),
                            "Verschil": diff,
                            "Status": status,
                        }
                    )

    return rows, counts


async def _auto_derive_otto_mapping(
    week: int,
    db: AsyncSession,
) -> tuple[dict[str, str], set[str]]:
    """Build loonnummer→SAP mapping for Otto directly from the data.

    Strategy:
    1. Collect the set of SAP IDs in invoice_lines for this week.
    2. For each kloklijst row, if loonnummers equals an invoice sap_id, map directly.
    3. Otherwise, look up the person in person_wagegroup_rates by normalized name
       (embedded numbers stripped); if their SAP appears in the invoice, map loon→SAP.
    """
    invoice_res = await db.execute(
        select(InvoiceLine.sap_id)
        .where(InvoiceLine.week_number == week, InvoiceLine.agency == "otto")
        .distinct()
    )
    invoice_saps: set[str] = {
        s for (s,) in ((_id_text(r[0]),) for r in invoice_res) if s
    }

    rates_res = await db.execute(
        select(PersonWagegroupRate.person_number, PersonWagegroupRate.normalized_name).where(
            PersonWagegroupRate.provider == "otto"
        )
    )
    rate_sap_by_norm_name: dict[str, str] = {}
    for sap_raw, norm_name in rates_res:
        sap = _id_text(sap_raw)
        if not sap or not norm_name:
            continue
        rate_sap_by_norm_name.setdefault(str(norm_name).strip().lower(), sap)

    klok_res = await db.execute(
        select(Kloklijst.loonnummers, Kloklijst.personeelsnummer, Kloklijst.naam)
        .where(
            Kloklijst.week_number == week,
            Kloklijst.agency == "otto",
            Kloklijst.naam.isnot(None),
        )
        .distinct()
    )
    klok_rows = list(klok_res)

    # Detect loonnummer collisions: a given loonnummer is ambiguous when it is
    # attached to multiple distinct (personeelsnummer, name) combinations in the
    # kloklijst. In that case we refuse to use a direct loon→SAP shortcut and
    # let name-based fallback disambiguate per row.
    loon_people: dict[str, set[tuple[str, str]]] = defaultdict(set)
    for loon_raw, pnummer_raw, naam in klok_rows:
        loon = _id_text(loon_raw)
        if not loon:
            continue
        pnummer = _id_text(pnummer_raw) or ""
        cleaned = _strip_embedded_numbers(str(naam or ""))
        loon_people[loon].add((pnummer, normalize_name(cleaned)))
    ambiguous_loons = {loon for loon, people in loon_people.items() if len(people) > 1}

    loonnummer_to_sap: dict[str, str] = {}
    mapped_sap_ids: set[str] = set()
    for loon_raw, _pnummer_raw, naam in klok_rows:
        loon = _id_text(loon_raw)
        if not loon or loon in ambiguous_loons:
            continue
        if loon in invoice_saps:
            loonnummer_to_sap.setdefault(loon, loon)
            mapped_sap_ids.add(loon)
            continue
        if not naam:
            continue
        cleaned = _strip_embedded_numbers(str(naam))
        norm = normalize_name(cleaned)
        sap = rate_sap_by_norm_name.get(norm)
        if sap and sap in invoice_saps:
            loonnummer_to_sap.setdefault(loon, sap)
            mapped_sap_ids.add(sap)
    return loonnummer_to_sap, mapped_sap_ids


async def run_validation(
    week: str,
    db: AsyncSession,
    agency: str,
    confirmed_same_pairs: list[tuple[str, str]] | None = None,
    confirmed_diff_pairs: list[tuple[str, str]] | None = None,
) -> dict:
    week_int = int(week)
    provider_suffix = "flex" if agency == "flexspecialisten" else agency
    output_file = f"output/{week} validation_hours_{provider_suffix}.csv"
    output_file_daily = f"output/{week} validation_hours_{provider_suffix}_daily.csv"

    loonnummer_to_sap: dict[str, str] = {}
    mapped_sap_ids: set[str] = set()
    if agency == "otto":
        loonnummer_to_sap, mapped_sap_ids = await load_verified_otto_mapping(db)
        auto_loon, auto_sap = await _auto_derive_otto_mapping(week_int, db)
        for loon, sap in auto_loon.items():
            loonnummer_to_sap.setdefault(loon, sap)
        mapped_sap_ids.update(auto_sap)

    factuur = await _load_factuur_hours_db(week_int, db, agency, mapped_sap_ids)
    kloklijst = await _load_kloklijst_hours_db(week_int, db, agency, loonnummer_to_sap)
    factuur_daily = await _load_factuur_hours_by_date_db(
        week_int, db, agency, mapped_sap_ids
    )
    kloklijst_daily = await _load_kloklijst_hours_by_date_db(
        week_int, db, agency, loonnummer_to_sap
    )

    if not factuur:
        raise ValueError(
            f"Geen factuurregels gevonden in de database voor week {week} (agency={agency})."
        )
    if not kloklijst:
        raise ValueError(
            f"Geen kloklijstregels gevonden in de database voor week {week} (agency={agency})."
        )

    confirmed_same_pairs = confirmed_same_pairs or []
    confirmed_diff_pairs = confirmed_diff_pairs or []
    confirmed_diff_pairs_set = {
        _pair_key(kloklijst_name, factuur_name)
        for kloklijst_name, factuur_name in confirmed_diff_pairs
    }

    all_names = set(factuur.keys()) | set(kloklijst.keys())
    alias_map = _build_alias_map(all_names, confirmed_same_pairs)

    factuur = _merge_week_hours_by_alias(factuur, alias_map)
    kloklijst = _merge_week_hours_by_alias(kloklijst, alias_map)
    factuur_daily = _merge_daily_hours_by_alias(factuur_daily, alias_map)
    kloklijst_daily = _merge_daily_hours_by_alias(kloklijst_daily, alias_map)

    factuur_display, kloklijst_display = await _build_name_display_maps_db(
        week_int, db, agency, mapped_sap_ids, loonnummer_to_sap
    )
    factuur_display = _apply_alias_to_display_map(factuur_display, alias_map)
    kloklijst_display = _apply_alias_to_display_map(kloklijst_display, alias_map)

    exact_person_match_count = len(set(factuur.keys()) & set(kloklijst.keys()))
    similar_people = _find_similar_name_pairs(
        factuur_names=set(factuur.keys()),
        kloklijst_names=set(kloklijst.keys()),
        factuur_display_map=factuur_display,
        kloklijst_display_map=kloklijst_display,
        confirmed_diff_pairs=confirmed_diff_pairs_set,
    )

    os.makedirs("output", exist_ok=True)

    # --- Weekly aggregated output ---
    key_display_map = {**kloklijst_display, **factuur_display}
    rows, counts = build_rows(
        factuur,
        kloklijst,
        include_date=False,
        display_names=key_display_map,
    )
    with open(output_file, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "Naam",
                "Code toeslag",
                "Factuur uren",
                "Kloklijst uren",
                "Verschil",
                "Status",
            ],
        )
        writer.writeheader()
        writer.writerows(rows)

    # --- Daily output ---
    rows_daily, counts_daily = build_rows(
        factuur_daily,
        kloklijst_daily,
        include_date=True,
        display_names=key_display_map,
    )
    with open(output_file_daily, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "Naam",
                "Datum",
                "Code toeslag",
                "Factuur uren",
                "Kloklijst uren",
                "Verschil",
                "Status",
            ],
        )
        writer.writeheader()
        writer.writerows(rows_daily)

    wagegroup_analysis: dict | None = None
    wagegroup_output_file: str | None = None
    rate_analysis: dict | None = None
    rate_output_file: str | None = None
    rate_histogram_file: str | None = None
    wagegroup_rate_analysis: dict | None = None
    wagegroup_rate_output_file: str | None = None

    if agency in ("otto", "flexspecialisten"):
        wagegroup_rate_analysis = await analyze_wagegroup_differences_by_rate(
            week=week_int,
            provider=agency,
            db=db,
            tolerance_eur=float(settings.rate_diff_tolerance_eur),
            output_dir="output",
        )
        wagegroup_rate_output_file = wagegroup_rate_analysis.get("outputFile")

    if agency == "otto":
        wagegroup_analysis = await analyze_otto_wagegroups(
            week=week_int,
            db=db,
            include_mismatches=True,
            max_items=100000,
        )
        wagegroup_output_file = f"output/{week} validation_wagegroups_otto.csv"
        _write_wagegroup_rows_csv(
            wagegroup_output_file,
            wagegroup_analysis.get("mismatches", []),
        )
        rate_analysis = await analyze_otto_rate_mismatches(
            week=week_int,
            db=db,
            tolerance_eur=float(settings.rate_diff_tolerance_eur),
            output_dir="output",
        )
        rate_output_file = rate_analysis.get("outputFile")
        rate_histogram_file = rate_analysis.get("histogramFile")

    return {
        "week": week,
        "agency": agency,
        "outputFileWeek": output_file,
        "outputFileDay": output_file_daily,
        "rowsWeek": rows,
        "rowsDay": rows_daily,
        "countsWeek": counts,
        "countsDay": counts_daily,
        "similarPeople": similar_people,
        "exactPersonMatchCount": exact_person_match_count,
        "wagegroupAnalysis": wagegroup_analysis,
        "wagegroupOutputFile": wagegroup_output_file,
        "rateAnalysis": rate_analysis,
        "rateOutputFile": rate_output_file,
        "rateHistogramFile": rate_histogram_file,
        "wagegroupRateAnalysis": wagegroup_rate_analysis,
        "wagegroupRateOutputFile": wagegroup_rate_output_file,
    }


def _fmt_value(value) -> str:
    if value is None or value == "":
        return "-"
    return str(value)


def _fmt_hours(value) -> str:
    if value is None or value == "":
        return "-"
    try:
        number = float(value)
    except (TypeError, ValueError):
        return str(value)
    if number.is_integer():
        return str(int(number))
    return f"{number:.2f}".replace(".", ",")


def _fmt_money_2(value) -> str:
    if value is None or value == "":
        return "-"
    try:
        number = float(value)
    except (TypeError, ValueError):
        return str(value)
    return f"{number:.2f}"


def _dedupe_rate_mismatches(rows: list[dict]) -> list[dict]:
    grouped: dict[tuple[str, str, str, str], dict] = {}
    for row in rows:
        name = _fmt_value(row.get("name"))
        code = _fmt_value(row.get("invoiceCodeToeslag"))
        invoice_rate = _fmt_money_2(row.get("invoiceRate"))
        expected_rate = _fmt_money_2(row.get("expectedRate"))
        key = (name, code, invoice_rate, expected_rate)

        diff_raw = row.get("difference")
        try:
            diff = float(diff_raw)
        except (TypeError, ValueError):
            diff = None

        entry = grouped.get(key)
        if not entry:
            grouped[key] = {
                "name": name,
                "code": code,
                "invoice_rate": invoice_rate,
                "expected_rate": expected_rate,
                "count": 1,
                "min_diff": diff,
                "max_diff": diff,
            }
            continue

        entry["count"] += 1
        if diff is not None:
            if entry["min_diff"] is None or diff < entry["min_diff"]:
                entry["min_diff"] = diff
            if entry["max_diff"] is None or diff > entry["max_diff"]:
                entry["max_diff"] = diff

    deduped = list(grouped.values())
    deduped.sort(key=lambda r: (r["name"].lower(), r["code"].lower()))
    return deduped


def _dedupe_wagegroup_rate_mismatches(rows: list[dict]) -> list[dict]:
    grouped: dict[tuple[str, str, str], dict] = {}
    for row in rows:
        name = _fmt_value(row.get("name"))
        deduced = _fmt_value(row.get("deducedInvoiceWagegroup"))
        reference = _fmt_value(row.get("referenceWagegroup"))
        key = (name, deduced, reference)
        diff_raw = row.get("difference")
        try:
            diff = float(diff_raw)
        except (TypeError, ValueError):
            diff = None

        entry = grouped.get(key)
        if not entry:
            grouped[key] = {
                "name": name,
                "deduced": deduced,
                "reference": reference,
                "count": 1,
                "min_diff": diff,
                "max_diff": diff,
            }
            continue

        entry["count"] += 1
        if diff is not None:
            if entry["min_diff"] is None or diff < entry["min_diff"]:
                entry["min_diff"] = diff
            if entry["max_diff"] is None or diff > entry["max_diff"]:
                entry["max_diff"] = diff

    deduped = list(grouped.values())
    deduped.sort(
        key=lambda r: (r["name"].lower(), r["deduced"].lower(), r["reference"].lower())
    )
    return deduped


def _write_wagegroup_rows_csv(path: str, rows: list[dict]) -> None:
    fieldnames = [
        "sapId",
        "name",
        "invoiceWagegroup",
        "knownWagegroup",
        "status",
        "matchMethod",
    ]
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    "sapId": row.get("sapId", ""),
                    "name": row.get("name", ""),
                    "invoiceWagegroup": row.get("invoiceWagegroup", ""),
                    "knownWagegroup": row.get("knownWagegroup", ""),
                    "status": row.get("status", ""),
                    "matchMethod": row.get("matchMethod", ""),
                }
            )


def format_validation_email_body(result: dict) -> str:
    week = result["week"]
    provider_label = result.get("providerLabel")
    rows_week = result["rowsWeek"]
    week_mismatches = [row for row in rows_week if row.get("Status") != "OK"]
    wagegroup_analysis = result.get("wagegroupAnalysis") or {}
    wagegroup_rows = wagegroup_analysis.get("mismatches", [])
    wagegroup_mismatches = [
        row
        for row in wagegroup_rows
        if str(row.get("status", "")).strip() == "mismatch"
    ]
    wagegroup_missing = [
        row
        for row in wagegroup_rows
        if str(row.get("status", "")).strip() == "missing_known_wagegroup"
    ]
    rate_analysis = result.get("rateAnalysis") or {}
    rate_mismatches = rate_analysis.get("mismatches", []) or []
    tolerance_eur = rate_analysis.get("toleranceEur")
    wagegroup_rate_analysis = result.get("wagegroupRateAnalysis") or {}
    wagegroup_rate_mismatches = wagegroup_rate_analysis.get("mismatches", []) or []
    provider_hint = f" voor {provider_label}" if provider_label else ""
    if (
        not week_mismatches
        and not wagegroup_mismatches
        and not wagegroup_missing
        and not wagegroup_rate_mismatches
    ):
        return "\n".join(
            [
                "Goedemorgen,",
                "",
                (
                    "Voor week "
                    f"{week} hebben we{provider_hint} geen discrepanties "
                    "gevonden met onze kloklijsten."
                ),
                "",
                "Bedankt!",
            ]
        )

    lines = ["Goedemorgen,", ""]
    has_wage_like = bool(
        wagegroup_mismatches
        or wagegroup_missing
        or wagegroup_rate_mismatches
    )
    if week_mismatches and has_wage_like:
        lines.append(
            "Op bovenstaande factuur hebben we"
            f"{provider_hint} voor week "
            f"{week} de volgende discrepanties gevonden met onze kloklijsten en loongroep-referenties:"
        )
    elif week_mismatches:
        lines.append(
            "Op bovenstaande factuur hebben we"
            f"{provider_hint} voor week "
            f"{week} de volgende discrepanties gevonden met onze kloklijsten:"
        )
    else:
        lines.append(
            "Op bovenstaande factuur hebben we"
            f"{provider_hint} voor week "
            f"{week} de volgende afwijkingen gevonden in loongroepen:"
        )

    for row in week_mismatches:
        name = _fmt_value(row.get("Naam"))
        code = _fmt_value(row.get("Code toeslag"))
        factuur_hours = _fmt_hours(row.get("Factuur uren"))
        kloklijst_hours = _fmt_hours(row.get("Kloklijst uren"))
        status = row.get("Status")

        if status == "VERSCHIL":
            lines.append(
                f"- Bij {name} staat {factuur_hours} uur voor {code}, maar dit moet {kloklijst_hours} uur zijn."
            )
        elif status == "ALLEEN IN FACTUUR":
            lines.append(
                f"- Bij {name} staat {factuur_hours} uur voor {code} op de factuur, maar deze uren staan niet op onze kloklijsten."
            )
        elif status == "ALLEEN IN KLOKLIJST":
            lines.append(
                f"- Bij {name} staat {kloklijst_hours} uur voor {code} op onze kloklijsten, maar deze uren ontbreken op de factuur."
            )
        else:
            lines.append(
                f"- Bij {name} is een discrepantie gevonden voor {code} (status: {_fmt_value(status)})."
            )

    if wagegroup_mismatches:
        lines.append("")
        lines.append("Daarnaast hebben we de volgende loongroepafwijkingen gevonden:")
        for row in wagegroup_mismatches[:20]:
            name = _fmt_value(row.get("name"))
            invoice_wagegroup = _fmt_value(row.get("invoiceWagegroup"))
            known_wagegroup = _fmt_value(row.get("knownWagegroup"))
            lines.append(
                f"- Bij {name} staat loongroep {invoice_wagegroup} op de factuur, maar referentie is {known_wagegroup}."
            )
    if wagegroup_missing:
        lines.append("")
        lines.append("Daarnaast ontbreken loongroepreferenties voor de volgende personen:")
        for row in wagegroup_missing[:20]:
            name = _fmt_value(row.get("name"))
            sap_id = _fmt_value(row.get("sapId"))
            lines.append(
                f"- Bij {name} (SAP {sap_id}) is geen loongroepreferentie beschikbaar in /wagegroups."
            )
    if wagegroup_rate_mismatches:
        deduped_wagegroup_rate_mismatches = _dedupe_wagegroup_rate_mismatches(
            wagegroup_rate_mismatches
        )
        lines.append("")
        tol = wagegroup_rate_analysis.get("toleranceEur")
        tol_text = (
            f"{float(tol):.2f}".replace(".", ",") if tol is not None else "1,00"
        )
        lines.append(
            f"Daarnaast zijn er loongroepverschillen gevonden op basis van tariefmapping (tolerantie EUR {tol_text}):"
        )
        for row in deduped_wagegroup_rate_mismatches[:20]:
            min_diff = row.get("min_diff")
            max_diff = row.get("max_diff")
            if min_diff is None or max_diff is None:
                diff_text = "-"
            elif min_diff == max_diff:
                diff_text = f"{min_diff:.4f}"
            else:
                diff_text = f"{min_diff:.4f}-{max_diff:.4f}"
            count_text = f", {row['count']}x gezien" if row.get("count", 0) > 1 else ""
            lines.append(
                f"- Bij {row['name']} wijkt de loongroep af: factuur afgeleid {row['deduced']}, referentie {row['reference']} (rate-verschil {diff_text}{count_text})."
            )

    lines.extend(["", "Kan hier een correctie van worden gemaakt?", "", "Bedankt!"])
    return "\n".join(lines)


async def _main_async(week: str) -> None:
    from db import async_session

    async with async_session() as session:
        result = await run_validation(week, session, agency="otto")

    print(f"Resultaat geschreven naar: {result['outputFileWeek']}")
    print(
        f"  {len(result['rowsWeek'])} regels | OK: {result['countsWeek']['ok']} | Verschil: {result['countsWeek']['verschil']} | Alleen factuur: {result['countsWeek']['only_factuur']} | Alleen kloklijst: {result['countsWeek']['only_kloklijst']}"
    )
    print(f"Resultaat geschreven naar: {result['outputFileDay']}")
    print(
        f"  {len(result['rowsDay'])} regels | OK: {result['countsDay']['ok']} | Verschil: {result['countsDay']['verschil']} | Alleen factuur: {result['countsDay']['only_factuur']} | Alleen kloklijst: {result['countsDay']['only_kloklijst']}"
    )


def main():
    parser = argparse.ArgumentParser(
        description="Validate hours between OTTO invoice and kloklijst."
    )
    parser.add_argument(
        "--week", required=True, help="Week number in YYYYww format, e.g. 202551"
    )
    args = parser.parse_args()
    asyncio.run(_main_async(args.week))


if __name__ == "__main__":
    main()

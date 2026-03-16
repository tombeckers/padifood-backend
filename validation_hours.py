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
from collections import defaultdict
from difflib import SequenceMatcher

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from models import InvoiceLine, Kloklijst

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

FUZZY_MATCH_THRESHOLD = 90


def normalize_name(name: str) -> str:
    """Lowercase and sort words so 'Dawid Kutermak' == 'Kutermak Dawid'."""
    return " ".join(sorted(name.lower().replace("-", " ").split()))


def _pair_key(kloklijst_name: str, factuur_name: str) -> tuple[str, str]:
    return (normalize_name(kloklijst_name), normalize_name(factuur_name))


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
        left_norm = normalize_name(left)
        right_norm = normalize_name(right)
        if left_norm not in parent or right_norm not in parent:
            continue
        union(left_norm, right_norm)

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


def _find_similar_name_pairs(
    factuur_names: set[str],
    kloklijst_names: set[str],
    factuur_display_map: dict[str, str],
    kloklijst_display_map: dict[str, str],
    confirmed_diff_pairs: set[tuple[str, str]],
) -> list[tuple[str, str]]:
    similar_pairs: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()

    kloklijst_only = sorted(kloklijst_names - factuur_names)
    factuur_only = sorted(factuur_names - kloklijst_names)

    for kloklijst_name in kloklijst_only:
        for factuur_name in factuur_only:
            key = (kloklijst_name, factuur_name)
            if key in confirmed_diff_pairs:
                continue
            if _fuzzy_score(kloklijst_name, factuur_name) <= FUZZY_MATCH_THRESHOLD:
                continue
            display_pair = (
                kloklijst_display_map.get(kloklijst_name, kloklijst_name),
                factuur_display_map.get(factuur_name, factuur_name),
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
    week: int, db: AsyncSession
) -> dict[str, dict[str, float]]:
    """Returns {normalized_name: {code_toeslag: total_uren}} from invoice_lines."""
    result = await db.execute(
        select(InvoiceLine).where(InvoiceLine.week_number == week)
    )
    totals: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    for row in result.scalars().all():
        if not row.naam or not row.code_toeslag:
            continue
        totals[normalize_name(row.naam)][row.code_toeslag] += row.totaal_uren
    return {k: dict(v) for k, v in totals.items()}


async def _load_kloklijst_hours_db(
    week: int, db: AsyncSession
) -> dict[str, dict[str, float]]:
    """Returns {normalized_name: {col_name: total_uren}} from kloklijst (agency=otto)."""
    result = await db.execute(
        select(Kloklijst).where(
            Kloklijst.week_number == week,
            Kloklijst.agency == "otto",
            Kloklijst.datum.isnot(None),
        )
    )
    totals: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    for row in result.scalars().all():
        if not row.naam:
            continue
        norm = normalize_name(row.naam)
        for col_name, field in KLOKLIJST_COL_TO_FIELD.items():
            val = getattr(row, field, None)
            if val is not None and val != 0:
                totals[norm][col_name] += val
    return {k: dict(v) for k, v in totals.items()}


async def _load_factuur_hours_by_date_db(
    week: int, db: AsyncSession
) -> dict[str, dict[str, dict[str, float]]]:
    """Returns {normalized_name: {date: {code_toeslag: total_uren}}} from invoice_lines."""
    result = await db.execute(
        select(InvoiceLine).where(InvoiceLine.week_number == week)
    )
    totals: dict = defaultdict(lambda: defaultdict(lambda: defaultdict(float)))
    for row in result.scalars().all():
        if not row.naam or not row.code_toeslag or row.datum is None:
            continue
        totals[normalize_name(row.naam)][str(row.datum)][row.code_toeslag] += row.totaal_uren
    return totals


async def _load_kloklijst_hours_by_date_db(
    week: int, db: AsyncSession
) -> dict[str, dict[str, dict[str, float]]]:
    """Returns {normalized_name: {date: {col_name: total_uren}}} from kloklijst (agency=otto)."""
    result = await db.execute(
        select(Kloklijst).where(
            Kloklijst.week_number == week,
            Kloklijst.agency == "otto",
            Kloklijst.datum.isnot(None),
        )
    )
    totals: dict = defaultdict(lambda: defaultdict(lambda: defaultdict(float)))
    for row in result.scalars().all():
        if not row.naam:
            continue
        norm = normalize_name(row.naam)
        date_key = str(row.datum)
        for col_name, field in KLOKLIJST_COL_TO_FIELD.items():
            val = getattr(row, field, None)
            if val is not None and val != 0:
                totals[norm][date_key][col_name] += val
    return totals


async def _build_name_display_maps_db(
    week: int, db: AsyncSession
) -> tuple[dict[str, str], dict[str, str]]:
    """Returns (factuur_display_map, kloklijst_display_map): normalized_name → raw_name."""
    factuur_result = await db.execute(
        select(InvoiceLine.naam)
        .where(InvoiceLine.week_number == week)
        .distinct()
    )
    factuur_names: dict[str, str] = {}
    for (naam,) in factuur_result:
        if naam:
            factuur_names.setdefault(normalize_name(naam), naam)

    kloklijst_result = await db.execute(
        select(Kloklijst.naam)
        .where(
            Kloklijst.week_number == week,
            Kloklijst.agency == "otto",
            Kloklijst.naam.isnot(None),
        )
        .distinct()
    )
    kloklijst_names: dict[str, str] = {}
    for (naam,) in kloklijst_result:
        if naam:
            kloklijst_names.setdefault(normalize_name(naam), naam)

    return factuur_names, kloklijst_names


def _apply_alias_to_display_map(
    display_map: dict[str, str], alias_map: dict[str, str]
) -> dict[str, str]:
    remapped: dict[str, str] = {}
    for original_name, display_name in display_map.items():
        canonical = alias_map.get(original_name, original_name)
        remapped.setdefault(canonical, display_name)
    return remapped


def build_rows(factuur, kloklijst, include_date=False):
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

                base = {"Naam": name}
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


async def run_validation(
    week: str,
    db: AsyncSession,
    confirmed_same_pairs: list[tuple[str, str]] | None = None,
    confirmed_diff_pairs: list[tuple[str, str]] | None = None,
) -> dict:
    week_int = int(week)
    output_file = f"output/{week} validation_hours.csv"
    output_file_daily = f"output/{week} validation_hours_daily.csv"

    factuur = await _load_factuur_hours_db(week_int, db)
    kloklijst = await _load_kloklijst_hours_db(week_int, db)
    factuur_daily = await _load_factuur_hours_by_date_db(week_int, db)
    kloklijst_daily = await _load_kloklijst_hours_by_date_db(week_int, db)

    if not factuur:
        raise ValueError(f"Geen factuurregels gevonden in de database voor week {week}.")
    if not kloklijst:
        raise ValueError(f"Geen kloklijstregels gevonden in de database voor week {week}.")

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

    factuur_display, kloklijst_display = await _build_name_display_maps_db(week_int, db)
    factuur_display = _apply_alias_to_display_map(factuur_display, alias_map)
    kloklijst_display = _apply_alias_to_display_map(kloklijst_display, alias_map)

    similar_people = _find_similar_name_pairs(
        factuur_names=set(factuur.keys()),
        kloklijst_names=set(kloklijst.keys()),
        factuur_display_map=factuur_display,
        kloklijst_display_map=kloklijst_display,
        confirmed_diff_pairs=confirmed_diff_pairs_set,
    )

    os.makedirs("output", exist_ok=True)

    # --- Weekly aggregated output ---
    rows, counts = build_rows(factuur, kloklijst, include_date=False)
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
        factuur_daily, kloklijst_daily, include_date=True
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

    return {
        "week": week,
        "outputFileWeek": output_file,
        "outputFileDay": output_file_daily,
        "rowsWeek": rows,
        "rowsDay": rows_daily,
        "countsWeek": counts,
        "countsDay": counts_daily,
        "similarPeople": similar_people,
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


def format_validation_email_body(result: dict) -> str:
    week = result["week"]
    rows_week = result["rowsWeek"]
    week_mismatches = [row for row in rows_week if row.get("Status") != "OK"]
    if not week_mismatches:
        return "\n".join(
            [
                "Goedemorgen,",
                "",
                (
                    "Voor week "
                    f"{week} hebben we op bovenstaande factuur geen discrepanties "
                    "gevonden met onze kloklijsten."
                ),
                "",
                "Bedankt!",
            ]
        )

    lines = [
        "Goedemorgen,",
        "",
        (
            "Op bovenstaande factuur hebben we voor week "
            f"{week} de volgende discrepanties gevonden met onze kloklijsten:"
        ),
    ]

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

    lines.extend(["", "Kan hier een correctie van worden gemaakt?", "", "Bedankt!"])
    return "\n".join(lines)


async def _main_async(week: str) -> None:
    from db import async_session

    async with async_session() as session:
        result = await run_validation(week, session)

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

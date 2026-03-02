"""
validation_hours.py

Compares hours per employee per wage category between:
  - "Padifood specificatie - Export Factuur.csv"
      → sum of "Totaal uren" grouped by "Naam" + "Code toeslag"
  - "202551 Kloklijst Padifood Otto Workforce.csv"
      → sum of each hour-type column grouped by employee name

Names are normalized (lowercase, words sorted) before matching, because the two
files use different name orderings (firstname-lastname vs lastname-firstname).
"""

import argparse
import csv
import os
from difflib import SequenceMatcher
from collections import defaultdict

# Kloklijst columns that map to invoice wage categories
KLOKLIJST_HOUR_COLS = [
    "Norm uren Dag",
    "T133 Dag",
    "T135 Dag",
    "T200 Dag",
    "OW140 Week",
    "OW180 Dag",
    "OW200 Dag",
]

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


def _build_name_display_maps(
    factuur_file: str, kloklijst_file: str
) -> tuple[dict[str, str], dict[str, str]]:
    factuur_names: dict[str, str] = {}
    with open(factuur_file, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            raw_name = row.get("Naam", "").strip()
            if not raw_name:
                continue
            normalized = normalize_name(raw_name)
            factuur_names.setdefault(normalized, raw_name)

    kloklijst_names: dict[str, str] = {}
    current_name: str | None = None
    with open(kloklijst_file, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        reader.fieldnames = [h.strip() for h in reader.fieldnames]
        for row in reader:
            naam = row.get("Naam", "").strip()
            if naam:
                current_name = naam
            if not current_name:
                continue
            if not row.get("Datum", "").strip():
                continue
            normalized = normalize_name(current_name)
            kloklijst_names.setdefault(normalized, current_name)

    return factuur_names, kloklijst_names


def _apply_alias_to_display_map(
    display_map: dict[str, str], alias_map: dict[str, str]
) -> dict[str, str]:
    remapped: dict[str, str] = {}
    for original_name, display_name in display_map.items():
        canonical = alias_map.get(original_name, original_name)
        remapped.setdefault(canonical, display_name)
    return remapped


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


def load_factuur_hours(filepath: str) -> dict[str, dict[str, float]]:
    """
    Returns {normalized_name: {code_toeslag: total_uren}}.
    """
    totals: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    with open(filepath, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            naam = row.get("Naam", "").strip()
            code = row.get("Code toeslag", "").strip()
            uren_str = row.get("Totaal uren", "").strip()
            if not naam or not code or not uren_str:
                continue
            try:
                uren = float(uren_str.replace(",", "."))
            except ValueError:
                continue
            totals[normalize_name(naam)][code] += uren
    return {k: dict(v) for k, v in totals.items()}


def load_kloklijst_hours(filepath: str) -> dict[str, dict[str, float]]:
    """
    Returns {normalized_name: {col_name: total_uren}} for each column in
    KLOKLIJST_HOUR_COLS, forward-filling Naam and skipping summary rows.
    """
    totals: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    current_name: str | None = None

    with open(filepath, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        reader.fieldnames = [h.strip() for h in reader.fieldnames]

        for row in reader:
            naam = row.get("Naam", "").strip()
            if naam:
                current_name = naam

            if not current_name:
                continue

            # Skip summary rows — they have no Datum
            if not row.get("Datum", "").strip():
                continue

            norm = normalize_name(current_name)
            for col in KLOKLIJST_HOUR_COLS:
                val_str = row.get(col, "").strip()
                if not val_str:
                    continue
                try:
                    val = float(val_str.replace(",", "."))
                except ValueError:
                    continue
                if val != 0:
                    totals[norm][col] += val

    return {k: dict(v) for k, v in totals.items()}


def normalize_date(date_str: str) -> str:
    """Trim datetime strings like '2025-12-15 00:00:00' to just '2025-12-15'."""
    return date_str.strip().split(" ")[0]


def load_factuur_hours_by_date(filepath: str) -> dict[str, dict[str, dict[str, float]]]:
    """
    Returns {normalized_name: {date: {code_toeslag: total_uren}}}.
    """
    totals: dict = defaultdict(lambda: defaultdict(lambda: defaultdict(float)))
    with open(filepath, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            naam = row.get("Naam", "").strip()
            code = row.get("Code toeslag", "").strip()
            datum = row.get("Datum", "").strip()
            uren_str = row.get("Totaal uren", "").strip()
            if not naam or not code or not datum or not uren_str:
                continue
            try:
                uren = float(uren_str.replace(",", "."))
            except ValueError:
                continue
            totals[normalize_name(naam)][normalize_date(datum)][code] += uren
    return totals


def load_kloklijst_hours_by_date(
    filepath: str,
) -> dict[str, dict[str, dict[str, float]]]:
    """
    Returns {normalized_name: {date: {col_name: total_uren}}} for each column in
    KLOKLIJST_HOUR_COLS, forward-filling Naam and skipping summary rows.
    """
    totals: dict = defaultdict(lambda: defaultdict(lambda: defaultdict(float)))
    current_name: str | None = None

    with open(filepath, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        reader.fieldnames = [h.strip() for h in reader.fieldnames]

        for row in reader:
            naam = row.get("Naam", "").strip()
            if naam:
                current_name = naam

            if not current_name:
                continue

            datum = row.get("Datum", "").strip()
            if not datum:
                continue

            norm = normalize_name(current_name)
            date_key = normalize_date(datum)
            for col in KLOKLIJST_HOUR_COLS:
                val_str = row.get(col, "").strip()
                if not val_str:
                    continue
                try:
                    val = float(val_str.replace(",", "."))
                except ValueError:
                    continue
                if val != 0:
                    totals[norm][date_key][col] += val

    return totals


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


def run_validation(
    week: str,
    confirmed_same_pairs: list[tuple[str, str]] | None = None,
    confirmed_diff_pairs: list[tuple[str, str]] | None = None,
) -> dict:
    factuur_file = f"formatted_input/{week} Padifood specificatie - Export Factuur.csv"
    kloklijst_file = f"formatted_input/{week} Kloklijst Padifood Otto Workforce.csv"
    output_file = f"output/{week} validation_hours.csv"
    output_file_daily = f"output/{week} validation_hours_daily.csv"

    if not os.path.exists(factuur_file):
        raise FileNotFoundError(f"Bestand niet gevonden: {factuur_file}")
    if not os.path.exists(kloklijst_file):
        raise FileNotFoundError(f"Bestand niet gevonden: {kloklijst_file}")

    factuur = load_factuur_hours(factuur_file)
    kloklijst = load_kloklijst_hours(kloklijst_file)
    factuur_daily = load_factuur_hours_by_date(factuur_file)
    kloklijst_daily = load_kloklijst_hours_by_date(kloklijst_file)

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

    factuur_display, kloklijst_display = _build_name_display_maps(
        factuur_file, kloklijst_file
    )
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
        "inputFactuurFile": factuur_file,
        "inputKloklijstFile": kloklijst_file,
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


def main():
    parser = argparse.ArgumentParser(
        description="Validate hours between OTTO invoice and kloklijst."
    )
    parser.add_argument(
        "--week", required=True, help="Week number in YYYYww format, e.g. 202551"
    )
    args = parser.parse_args()
    week = args.week

    try:
        result = run_validation(week)
    except FileNotFoundError as e:
        print(f"FOUT: {e}")
        return

    print(f"Resultaat geschreven naar: {result['outputFileWeek']}")
    print(
        f"  {len(result['rowsWeek'])} regels | OK: {result['countsWeek']['ok']} | Verschil: {result['countsWeek']['verschil']} | Alleen factuur: {result['countsWeek']['only_factuur']} | Alleen kloklijst: {result['countsWeek']['only_kloklijst']}"
    )
    print(f"Resultaat geschreven naar: {result['outputFileDay']}")
    print(
        f"  {len(result['rowsDay'])} regels | OK: {result['countsDay']['ok']} | Verschil: {result['countsDay']['verschil']} | Alleen factuur: {result['countsDay']['only_factuur']} | Alleen kloklijst: {result['countsDay']['only_kloklijst']}"
    )


if __name__ == "__main__":
    main()

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


def normalize_name(name: str) -> str:
    """Lowercase and sort words so 'Dawid Kutermak' == 'Kutermak Dawid'."""
    return " ".join(sorted(name.lower().replace("-", " ").split()))


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


def run_validation(week: str) -> dict:
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

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

import csv
import os
from collections import defaultdict

FACTUUR_FILE = "formatted_input/Padifood specificatie - Export Factuur.csv"
KLOKLIJST_FILE = "formatted_input/202551 Kloklijst Padifood Otto Workforce.csv"
OUTPUT_FILE = "output/validation_hours.csv"

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


def main():
    if not os.path.exists(FACTUUR_FILE):
        print(f"ERROR: file not found: {FACTUUR_FILE}")
        return
    if not os.path.exists(KLOKLIJST_FILE):
        print(f"ERROR: file not found: {KLOKLIJST_FILE}")
        return

    factuur = load_factuur_hours(FACTUUR_FILE)
    kloklijst = load_kloklijst_hours(KLOKLIJST_FILE)

    all_names = sorted(set(factuur) | set(kloklijst))

    rows = []
    total_mismatches = 0
    total_only_factuur = 0
    total_only_kloklijst = 0
    total_ok = 0

    for name in all_names:
        f_cats = factuur.get(name, {})
        k_cats = kloklijst.get(name, {})
        all_cats = sorted(set(f_cats) | set(k_cats))

        for cat in all_cats:
            f_uren = f_cats.get(cat)
            k_uren = k_cats.get(cat)

            if f_uren is None:
                rows.append({
                    "Naam": name,
                    "Code toeslag": cat,
                    "Factuur uren": "",
                    "Kloklijst uren": round(k_uren, 2),
                    "Verschil": "",
                    "Status": "ALLEEN IN KLOKLIJST",
                })
                total_only_kloklijst += 1
            elif k_uren is None:
                rows.append({
                    "Naam": name,
                    "Code toeslag": cat,
                    "Factuur uren": round(f_uren, 2),
                    "Kloklijst uren": "",
                    "Verschil": "",
                    "Status": "ALLEEN IN FACTUUR",
                })
                total_only_factuur += 1
            else:
                diff = round(f_uren - k_uren, 2)
                status = "OK" if diff == 0 else "VERSCHIL"
                if diff != 0:
                    total_mismatches += 1
                else:
                    total_ok += 1
                rows.append({
                    "Naam": name,
                    "Code toeslag": cat,
                    "Factuur uren": round(f_uren, 2),
                    "Kloklijst uren": round(k_uren, 2),
                    "Verschil": diff,
                    "Status": status,
                })

    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["Naam", "Code toeslag", "Factuur uren", "Kloklijst uren", "Verschil", "Status"],
        )
        writer.writeheader()
        writer.writerows(rows)

    print(f"Resultaat geschreven naar: {OUTPUT_FILE}")
    print(f"\nSamenvatting ({len(rows)} regels):")
    print(f"  Overeenstemmend      : {total_ok}")
    print(f"  Uren verschillen     : {total_mismatches}")
    print(f"  Alleen in factuur    : {total_only_factuur}")
    print(f"  Alleen in kloklijst  : {total_only_kloklijst}")


if __name__ == "__main__":
    main()

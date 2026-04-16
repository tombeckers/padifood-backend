"""
Database inspection script.

Sections:
  1. Row counts — all tables
  2. Week coverage — which weeks have data per table/agency
  3. Identifier mapping — coverage and unverified/missing entries
  4. Wagegroup coverage — who has/lacks a wagegroup assignment
  5. Rate coverage — person-level vs rate-card-only coverage

Run: uv run python inspect_db.py [section]
     section: counts | weeks | mapping | wagegroups | rates | all (default)
"""

import asyncio
import sys

import asyncpg

from config import Settings


# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------

async def connect() -> asyncpg.Connection:
    url = Settings().postgres_database_url
    return await asyncpg.connect(url)


# ---------------------------------------------------------------------------
# Section helpers
# ---------------------------------------------------------------------------

def header(title: str):
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print(f"{'=' * 60}")


def subheader(title: str):
    print(f"\n  --- {title} ---")


def row_fmt(*cols, widths):
    return "  " + "  ".join(str(v).ljust(w) for v, w in zip(cols, widths))


# ---------------------------------------------------------------------------
# 1. Row counts
# ---------------------------------------------------------------------------

TABLES = [
    "kloklijst",
    "invoice_lines",
    "tarievensheet",
    "otto_rate_card",
    "otto_identifier_mapping",
    "person_wagegroups",
    "person_wagegroup_rates",
    "wagegroup_rate_card",
]


async def section_counts(conn: asyncpg.Connection):
    header("ROW COUNTS")
    print(row_fmt("table", "rows", widths=[35, 8]))
    print("  " + "-" * 45)
    for table in TABLES:
        n = await conn.fetchval(f"SELECT COUNT(*) FROM {table}")
        print(row_fmt(table, n, widths=[35, 8]))


# ---------------------------------------------------------------------------
# 2. Week coverage
# ---------------------------------------------------------------------------

async def section_weeks(conn: asyncpg.Connection):
    header("WEEK COVERAGE")

    # kloklijst
    subheader("kloklijst (by agency)")
    rows = await conn.fetch("""
        SELECT agency, MIN(week_number) AS min_week, MAX(week_number) AS max_week,
               COUNT(DISTINCT week_number) AS n_weeks,
               array_agg(DISTINCT week_number ORDER BY week_number) AS weeks
        FROM kloklijst
        GROUP BY agency ORDER BY agency
    """)
    print(row_fmt("agency", "min", "max", "n_weeks", "weeks", widths=[18, 7, 7, 8, 40]))
    print("  " + "-" * 82)
    for r in rows:
        print(row_fmt(r["agency"], r["min_week"], r["max_week"], r["n_weeks"],
                      str(list(r["weeks"])), widths=[18, 7, 7, 8, 40]))

    # invoice_lines
    subheader("invoice_lines (by agency)")
    rows = await conn.fetch("""
        SELECT agency, MIN(week_number) AS min_week, MAX(week_number) AS max_week,
               COUNT(DISTINCT week_number) AS n_weeks,
               array_agg(DISTINCT week_number ORDER BY week_number) AS weeks
        FROM invoice_lines
        GROUP BY agency ORDER BY agency
    """)
    print(row_fmt("agency", "min", "max", "n_weeks", "weeks", widths=[18, 7, 7, 8, 40]))
    print("  " + "-" * 82)
    for r in rows:
        print(row_fmt(r["agency"], r["min_week"], r["max_week"], r["n_weeks"],
                      str(list(r["weeks"])), widths=[18, 7, 7, 8, 40]))

    # tarievensheet
    subheader("tarievensheet")
    rows = await conn.fetch("""
        SELECT MIN(week_number) AS min_week, MAX(week_number) AS max_week,
               COUNT(DISTINCT week_number) AS n_weeks,
               array_agg(DISTINCT week_number ORDER BY week_number) AS weeks
        FROM tarievensheet
    """)
    for r in rows:
        print(f"  weeks: {list(r['weeks'])}  (min={r['min_week']}, max={r['max_week']}, n={r['n_weeks']})")

    # weeks present in kloklijst but not in invoice_lines (and vice versa) for otto
    subheader("OTTO weeks in kloklijst but NOT in invoice_lines")
    rows = await conn.fetch("""
        SELECT DISTINCT k.week_number
        FROM kloklijst k
        WHERE k.agency = 'otto'
          AND k.week_number NOT IN (
              SELECT DISTINCT week_number FROM invoice_lines WHERE agency = 'otto'
          )
        ORDER BY k.week_number
    """)
    if rows:
        print("  " + ", ".join(str(r["week_number"]) for r in rows))
    else:
        print("  (none — all kloklijst weeks have invoice data)")

    subheader("OTTO weeks in invoice_lines but NOT in kloklijst")
    rows = await conn.fetch("""
        SELECT DISTINCT i.week_number
        FROM invoice_lines i
        WHERE i.agency = 'otto'
          AND i.week_number NOT IN (
              SELECT DISTINCT week_number FROM kloklijst WHERE agency = 'otto'
          )
        ORDER BY i.week_number
    """)
    if rows:
        print("  " + ", ".join(str(r["week_number"]) for r in rows))
    else:
        print("  (none — all invoice weeks have kloklijst data)")


# ---------------------------------------------------------------------------
# 3. Identifier mapping
# ---------------------------------------------------------------------------

async def section_mapping(conn: asyncpg.Connection):
    header("IDENTIFIER MAPPING  (otto_identifier_mapping)")

    # Summary
    total = await conn.fetchval("SELECT COUNT(*) FROM otto_identifier_mapping")
    verified = await conn.fetchval("SELECT COUNT(*) FROM otto_identifier_mapping WHERE verified = true")
    unverified = await conn.fetchval("SELECT COUNT(*) FROM otto_identifier_mapping WHERE verified = false")
    print(f"  total={total}  verified={verified}  unverified={unverified}")

    # By match type
    subheader("By match_type")
    rows = await conn.fetch("""
        SELECT match_type, verified, COUNT(*) n
        FROM otto_identifier_mapping
        GROUP BY match_type, verified
        ORDER BY match_type, verified
    """)
    print(row_fmt("match_type", "verified", "n", widths=[25, 10, 6]))
    print("  " + "-" * 43)
    for r in rows:
        print(row_fmt(r["match_type"], r["verified"], r["n"], widths=[25, 10, 6]))

    # SAP IDs on the invoice that have no mapping entry
    subheader("Invoice SAP IDs with no mapping entry (sample, max 20)")
    rows = await conn.fetch("""
        SELECT DISTINCT i.sap_id, i.naam
        FROM invoice_lines i
        WHERE i.agency = 'otto'
          AND NOT EXISTS (
              SELECT 1 FROM otto_identifier_mapping m WHERE m.sap_id = i.sap_id
          )
        ORDER BY i.naam
        LIMIT 20
    """)
    if rows:
        print(row_fmt("sap_id", "naam", widths=[15, 35]))
        print("  " + "-" * 52)
        for r in rows:
            print(row_fmt(r["sap_id"], r["naam"], widths=[15, 35]))
    else:
        print("  (all invoice SAP IDs have a mapping entry)")

    # Kloklijst loonnummers with no mapping
    subheader("Kloklijst loonnummers with no mapping entry (sample, max 20)")
    rows = await conn.fetch("""
        SELECT DISTINCT k.loonnummers, k.naam
        FROM kloklijst k
        WHERE k.agency = 'otto'
          AND k.loonnummers IS NOT NULL
          AND NOT EXISTS (
              SELECT 1 FROM otto_identifier_mapping m
              WHERE m.kloklijst_loonnummer = k.loonnummers::text
          )
        ORDER BY k.naam
        LIMIT 20
    """)
    if rows:
        print(row_fmt("loonnummer", "naam", widths=[12, 35]))
        print("  " + "-" * 49)
        for r in rows:
            print(row_fmt(r["loonnummers"], r["naam"], widths=[12, 35]))
    else:
        print("  (all kloklijst loonnummers have a mapping entry)")


# ---------------------------------------------------------------------------
# 4. Wagegroup coverage
# ---------------------------------------------------------------------------

async def section_wagegroups(conn: asyncpg.Connection):
    header("WAGEGROUP COVERAGE  (person_wagegroups)")

    total = await conn.fetchval("SELECT COUNT(*) FROM person_wagegroups")
    verified = await conn.fetchval("SELECT COUNT(*) FROM person_wagegroups WHERE verified = true")
    print(f"  total={total}  verified={verified}  unverified={total - verified}")

    # By provider
    subheader("By provider")
    rows = await conn.fetch("""
        SELECT provider, verified, COUNT(*) n
        FROM person_wagegroups
        GROUP BY provider, verified ORDER BY provider, verified
    """)
    print(row_fmt("provider", "verified", "n", widths=[20, 10, 6]))
    print("  " + "-" * 38)
    for r in rows:
        print(row_fmt(r["provider"], r["verified"], r["n"], widths=[20, 10, 6]))

    # Wagegroup distribution
    subheader("Wagegroup distribution")
    rows = await conn.fetch("""
        SELECT provider, wagegroup, COUNT(*) n
        FROM person_wagegroups
        GROUP BY provider, wagegroup ORDER BY provider, wagegroup
    """)
    print(row_fmt("provider", "wagegroup", "n", widths=[20, 15, 6]))
    print("  " + "-" * 43)
    for r in rows:
        print(row_fmt(r["provider"], r["wagegroup"], r["n"], widths=[20, 15, 6]))

    # Invoice SAP IDs with no wagegroup
    subheader("OTTO invoice SAP IDs with no wagegroup (sample, max 20)")
    rows = await conn.fetch("""
        SELECT DISTINCT i.sap_id, i.naam
        FROM invoice_lines i
        WHERE i.agency = 'otto'
          AND NOT EXISTS (
              SELECT 1 FROM person_wagegroups w
              WHERE w.provider = 'otto' AND w.person_number = i.sap_id
          )
        ORDER BY i.naam
        LIMIT 20
    """)
    if rows:
        print(row_fmt("sap_id", "naam", widths=[15, 35]))
        print("  " + "-" * 52)
        for r in rows:
            print(row_fmt(r["sap_id"], r["naam"], widths=[15, 35]))
    else:
        print("  (all invoice SAP IDs have a wagegroup)")


# ---------------------------------------------------------------------------
# 5. Rate coverage
# ---------------------------------------------------------------------------

async def section_rates(conn: asyncpg.Connection):
    header("RATE COVERAGE  (person_wagegroup_rates / wagegroup_rate_card)")

    # person_wagegroup_rates summary
    subheader("person_wagegroup_rates")
    rows = await conn.fetch("""
        SELECT provider, COUNT(DISTINCT person_number) n_persons,
               COUNT(*) n_rows,
               array_agg(DISTINCT rate_key ORDER BY rate_key) rate_keys
        FROM person_wagegroup_rates
        GROUP BY provider ORDER BY provider
    """)
    for r in rows:
        print(f"  provider={r['provider']}  persons={r['n_persons']}  rows={r['n_rows']}")
        print(f"    rate_keys: {list(r['rate_keys'])}")

    # wagegroup_rate_card summary
    subheader("wagegroup_rate_card")
    rows = await conn.fetch("""
        SELECT provider, COUNT(DISTINCT (schaal, tarief)) n_combos,
               COUNT(*) n_rows,
               array_agg(DISTINCT schaal ORDER BY schaal) schalen
        FROM wagegroup_rate_card
        GROUP BY provider ORDER BY provider
    """)
    for r in rows:
        print(f"  provider={r['provider']}  schaal/tarief combos={r['n_combos']}  rows={r['n_rows']}")
        print(f"    schalen: {list(r['schalen'])}")

    # Persons with wagegroup but no person-level rate
    subheader("Persons with wagegroup but no person-level rate entry")
    rows = await conn.fetch("""
        SELECT w.provider, w.person_number, w.name, w.wagegroup
        FROM person_wagegroups w
        WHERE NOT EXISTS (
            SELECT 1 FROM person_wagegroup_rates r
            WHERE r.provider = w.provider AND r.person_number = w.person_number
        )
        ORDER BY w.provider, w.name
        LIMIT 30
    """)
    if rows:
        print(row_fmt("provider", "person_number", "name", "wagegroup", widths=[18, 15, 30, 12]))
        print("  " + "-" * 77)
        for r in rows:
            print(row_fmt(r["provider"], r["person_number"], r["name"], r["wagegroup"],
                          widths=[18, 15, 30, 12]))
    else:
        print("  (all persons with a wagegroup also have person-level rates)")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

SECTIONS = {
    "counts": section_counts,
    "weeks": section_weeks,
    "mapping": section_mapping,
    "wagegroups": section_wagegroups,
    "rates": section_rates,
}


async def main():
    arg = sys.argv[1] if len(sys.argv) > 1 else "all"
    conn = await connect()
    try:
        if arg == "all":
            for fn in SECTIONS.values():
                await fn(conn)
        elif arg in SECTIONS:
            await SECTIONS[arg](conn)
        else:
            print(f"Unknown section '{arg}'. Choose: {', '.join(SECTIONS)} or 'all'")
    finally:
        await conn.close()
    print()


asyncio.run(main())

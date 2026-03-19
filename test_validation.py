"""
Temporary test script — run with: python test_validation.py
"""

import asyncio
from db import async_session, engine
from sqlalchemy import text
from validation_hours import run_validation


async def main():
    # 1. Check available weeks in both tables
    async with engine.connect() as conn:
        print("=== Available weeks ===")
        for table in ["invoice_lines", "kloklijst"]:
            result = await conn.execute(
                text(f"SELECT DISTINCT week_number FROM {table} ORDER BY 1")
            )
            weeks = [r[0] for r in result]
            print(f"  {table}: {weeks}")

        print()

        # 2. Row counts
        print("=== Row counts ===")
        for table in ["invoice_lines", "kloklijst", "tarievensheet", "otto_rate_card"]:
            result = await conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
            print(f"  {table}: {result.scalar()}")

        print()

        # 3. Sample invoice_lines
        result = await conn.execute(
            text(
                "SELECT week_number, naam, code_toeslag, totaal_uren "
                "FROM invoice_lines LIMIT 3"
            )
        )
        print("=== invoice_lines sample ===")
        for r in result:
            print(f"  {r}")

        print()

        # 4. Sample kloklijst (only rows with a date, otto agency)
        result = await conn.execute(
            text(
                "SELECT week_number, agency, naam, datum, norm_uren_dag "
                "FROM kloklijst WHERE datum IS NOT NULL AND agency = 'otto' LIMIT 3"
            )
        )
        print("=== kloklijst sample (otto, datum not null) ===")
        for r in result:
            print(f"  {r}")

    print()

    # 5. Run validation for the first available week
    async with engine.connect() as conn:
        result = await conn.execute(text("SELECT MIN(week_number) FROM invoice_lines"))
        week_int = result.scalar()

    if not week_int:
        print("No data found in invoice_lines — upload a file first.")
        return

    week = str(week_int)
    print(f"=== Running validation for week {week} ===")
    async with async_session() as db:
        result = await run_validation(week, db, agency="otto")

    counts = result["countsWeek"]
    print(f"  Rows:          {len(result['rowsWeek'])}")
    print(f"  OK:            {counts['ok']}")
    print(f"  Verschil:      {counts['verschil']}")
    print(f"  Alleen factuur:{counts['only_factuur']}")
    print(f"  Alleen kloklijst:{counts['only_kloklijst']}")
    print(f"  Similar people:{result['similarPeople']}")
    print(f"  Output week:   {result['outputFileWeek']}")
    print(f"  Output day:    {result['outputFileDay']}")


asyncio.run(main())

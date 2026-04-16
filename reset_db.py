"""
Empty all application tables (TRUNCATE ... RESTART IDENTITY CASCADE).

Run: uv run python reset_db.py
     uv run python reset_db.py --yes   # skip confirmation prompt
"""

import asyncio
import sys

import asyncpg

from config import Settings

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


async def main():
    skip_confirm = "--yes" in sys.argv

    if not skip_confirm:
        print("This will TRUNCATE all data from the following tables:")
        for t in TABLES:
            print(f"  {t}")
        answer = input("\nType 'yes' to continue: ").strip().lower()
        if answer != "yes":
            print("Aborted.")
            return

    url = Settings().postgres_database_url
    conn = await asyncpg.connect(url)
    try:
        tables_sql = ", ".join(TABLES)
        await conn.execute(f"TRUNCATE {tables_sql} RESTART IDENTITY CASCADE")
        print("Done — all tables emptied.")
    finally:
        await conn.close()


asyncio.run(main())

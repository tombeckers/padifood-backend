"""Quick DB table count checker. Run: python check_db.py"""
import asyncio
import asyncpg
import os
from dotenv import load_dotenv

load_dotenv()

SQL = """
SELECT t, agency, n FROM (
  SELECT 1 ord, 'kloklijst' t, agency, COUNT(*)::int n FROM kloklijst GROUP BY agency
  UNION ALL SELECT 2, 'invoice_lines', agency, COUNT(*)::int FROM invoice_lines GROUP BY agency
  UNION ALL SELECT 3, 'otto_identifier_mapping', NULL, COUNT(*)::int FROM otto_identifier_mapping
  UNION ALL SELECT 4, 'otto_rate_card', NULL, COUNT(*)::int FROM otto_rate_card
  UNION ALL SELECT 5, 'person_wagegroups', NULL, COUNT(*)::int FROM person_wagegroups
  UNION ALL SELECT 6, 'tarievensheet', NULL, COUNT(*)::int FROM tarievensheet
) q ORDER BY ord, agency NULLS LAST
"""

TABLES = ["kloklijst", "invoice_lines", "otto_identifier_mapping", "otto_rate_card", "person_wagegroups", "tarievensheet"]


async def main():
    url = os.getenv("POSTGRES_DATABASE_URL")
    conn = await asyncpg.connect(url)
    rows = await conn.fetch(SQL)
    await conn.close()

    seen = set()
    print(f"{'table':<30} {'agency':<15} {'rows':>6}")
    print("-" * 55)
    for r in rows:
        key = r["t"]
        seen.add(key)
        print(f"{r['t']:<30} {str(r['agency'] or ''):<15} {r['n']:>6}")

    # Print 0 for tables with no rows at all (GROUP BY returns nothing on empty)
    for t in TABLES:
        if t not in seen:
            print(f"{t:<30} {'(empty)':<15} {0:>6}")


asyncio.run(main())

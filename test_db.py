import asyncio
from db import engine
from sqlalchemy import text


async def test():
    async with engine.connect() as conn:
        result = await conn.execute(text("SELECT 1"))
        print("Connection OK:", result.scalar())
        result = await conn.execute(
            text(
                "SELECT table_name FROM information_schema.tables WHERE table_schema='public'"
            )
        )
        print(result.fetchall())


asyncio.run(test())

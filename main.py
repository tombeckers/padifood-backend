from contextlib import asynccontextmanager
from typing import List

from fastapi import Depends, FastAPI, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from config import Settings

settings = Settings()

# Support postgresql:// URL by converting to postgresql+asyncpg://
database_url = settings.postgres_database_url
if database_url.startswith("postgresql://"):
    database_url = database_url.replace("postgresql://", "postgresql+asyncpg://", 1)

engine = create_async_engine(database_url, echo=False)
async_session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    await engine.dispose()


app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


async def get_db() -> AsyncSession:
    async with async_session() as session:
        yield session


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/upload")
async def upload(
    files: List[UploadFile] = File(...),
    db: AsyncSession = Depends(get_db),
):
    print("upload called, db:", db)
    results = []

    for file in files:
        content = await file.read()
        results.append(
            {
                "filename": file.filename,
                "content_type": file.content_type,
                "size": len(content),
            }
        )

    total_size = sum(result["size"] for result in results)

    return {
        "emailBody": f"Test resultaat: \n\nTotal size: {total_size} bytes from {len(files)} files."
    }

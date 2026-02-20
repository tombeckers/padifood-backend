from contextlib import asynccontextmanager
import io
import os
from pathlib import Path
from datetime import date, datetime
from typing import List
import re

from fastapi import Depends, FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.security import APIKeyHeader
from openpyxl import load_workbook
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from config import Settings
from convert import convert_input
from validation_hours import format_validation_email_body, run_validation

settings = Settings()

API_KEY_HEADER = APIKeyHeader(name="X-API-Key", auto_error=False)

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
BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = (BASE_DIR / "output").resolve()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


async def verify_api_key(api_key: str = Depends(API_KEY_HEADER)):
    if not api_key or api_key != settings.backend_api_key:
        raise HTTPException(
            status_code=401, detail="Ontbrekende of ongeldige API-sleutel"
        )


async def get_db() -> AsyncSession:
    async with async_session() as session:
        yield session


@app.get("/health")
def health():
    return {"status": "ok"}


class DownloadRequest(BaseModel):
    fileName: str


@app.post("/download")
async def download(payload: DownloadRequest):
    requested = payload.fileName.strip()
    if not requested:
        raise HTTPException(status_code=400, detail="fileName is required.")

    candidate_path = Path(requested)
    if not candidate_path.is_absolute():
        candidate_path = (BASE_DIR / candidate_path).resolve()
    else:
        candidate_path = candidate_path.resolve()

    try:
        candidate_path.relative_to(OUTPUT_DIR)
    except ValueError as e:
        raise HTTPException(
            status_code=400,
            detail="Ongeldig bestandspad.",
        ) from e

    if not candidate_path.exists() or not candidate_path.is_file():
        raise HTTPException(status_code=404, detail="Bestand niet gevonden.")

    return FileResponse(
        path=str(candidate_path),
        filename=candidate_path.name,
        media_type="application/octet-stream",
    )


@app.post("/upload")
async def upload(
    files: List[UploadFile] = File(...),
    db: AsyncSession = Depends(get_db),
    _: None = Depends(verify_api_key),
):
    _ = db  # dependency kept for compatibility
    input_dir = "input"
    os.makedirs(input_dir, exist_ok=True)

    if len(files) != 2:
        raise HTTPException(
            status_code=400,
            detail="Precies 2 bestanden zijn vereist: een kloklijst en een factuurbestand (.xlsx).",
        )

    uploaded = []
    for file in files:
        if not file.filename:
            raise HTTPException(
                status_code=400, detail="Geüpload bestand heeft geen bestandsnaam."
            )
        if not file.filename.lower().endswith(".xlsx"):
            raise HTTPException(
                status_code=400,
                detail=f"Alleen .xlsx-bestanden worden ondersteund: {file.filename}",
            )
        content = await file.read()
        uploaded.append({"filename": file.filename, "content": content})

    def classify(data):
        kloklijst = None
        factuur = None
        for item in data:
            lower = item["filename"].lower()
            is_kloklijst = "kloklijst" in lower
            is_factuur = "factuur" in lower or "specificatie" in lower
            if is_kloklijst and is_factuur:
                raise HTTPException(
                    status_code=400,
                    detail=f"Onduidelijk bestandstype voor bestandsnaam: {item['filename']}",
                )
            if is_kloklijst:
                if kloklijst is not None:
                    raise HTTPException(
                        status_code=400,
                        detail="Meerdere kloklijstbestanden gevonden; precies één verwacht.",
                    )
                kloklijst = item
            elif is_factuur:
                if factuur is not None:
                    raise HTTPException(
                        status_code=400,
                        detail="Meerdere factuurbestanden gevonden; precies één verwacht.",
                    )
                factuur = item

        if kloklijst is None or factuur is None:
            raise HTTPException(
                status_code=400,
                detail="Kon geüploade bestanden niet indelen als kloklijst en factuur.",
            )
        return kloklijst, factuur

    def extract_week_from_filename(filename: str) -> str | None:
        match = re.match(r"^\s*(\d{6})\b", filename)
        return match.group(1) if match else None

    def parse_excel_date(value) -> date | None:
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, date):
            return value
        if value is None:
            return None
        text = str(value).strip()
        if not text:
            return None
        text = text.split(" ")[0]
        date_formats = [
            "%Y-%m-%d",
            "%d-%m-%Y",
            "%d/%m/%Y",
            "%Y/%m/%d",
        ]
        for fmt in date_formats:
            try:
                return datetime.strptime(text, fmt).date()
            except ValueError:
                continue
        return None

    def extract_week_from_factuur(content: bytes) -> str | None:
        wb = load_workbook(io.BytesIO(content), data_only=True)
        if "Export Factuur" not in wb.sheetnames:
            return None

        ws = wb["Export Factuur"]
        rows = list(ws.iter_rows(values_only=True))
        if not rows:
            return None

        header = [str(c).strip() if c is not None else "" for c in rows[0]]
        if "Datum" not in header:
            return None

        datum_idx = header.index("Datum")
        for row in rows[1:]:
            if datum_idx >= len(row):
                continue
            parsed = parse_excel_date(row[datum_idx])
            if parsed is None:
                continue
            iso = parsed.isocalendar()
            return f"{iso[0]}{iso[1]:02d}"
        return None

    def build_prefixed_filename(original_filename: str, week: str) -> str:
        fname = os.path.basename(original_filename)
        stem = re.sub(r"\.xlsx$", "", fname, flags=re.IGNORECASE)
        stem = re.sub(r"^\s*\d{6}\b[\s,._-]*", "", stem)
        stem = stem.replace(",", " ")
        stem = " ".join(stem.split()).strip()
        if not stem:
            raise HTTPException(
                status_code=400,
                detail=f"Kon geen geldige bestandsnaam afleiden uit: {original_filename}",
            )
        return f"{week} {stem}.xlsx"

    kloklijst_upload, factuur_upload = classify(uploaded)

    week_from_kloklijst = extract_week_from_filename(kloklijst_upload["filename"])
    if not week_from_kloklijst:
        raise HTTPException(
            status_code=400,
            detail="Kon weeknummer (YYYYww) niet vinden in de bestandsnaam van de kloklijst.",
        )

    week_from_factuur = extract_week_from_factuur(factuur_upload["content"])
    if not week_from_factuur:
        raise HTTPException(
            status_code=400,
            detail="Kon weeknummer niet bepalen uit kolom Datum in sheet Export Factuur.",
        )

    if week_from_kloklijst != week_from_factuur:
        raise HTTPException(
            status_code=400,
            detail=(
                "Weeknummer komt niet overeen tussen kloklijst en factuur: "
                f"{week_from_kloklijst} != {week_from_factuur}"
            ),
        )

    week = week_from_factuur

    kloklijst_name = build_prefixed_filename(kloklijst_upload["filename"], week)
    factuur_name = build_prefixed_filename(factuur_upload["filename"], week)
    kloklijst_path = os.path.join(input_dir, kloklijst_name)
    factuur_path = os.path.join(input_dir, factuur_name)

    with open(kloklijst_path, "wb") as f:
        f.write(kloklijst_upload["content"])
    with open(factuur_path, "wb") as f:
        f.write(factuur_upload["content"])

    try:
        convert_input(kloklijst_name, factuur_name)
        validation_result = run_validation(week)
    except FileNotFoundError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Onverwachte fout tijdens het verwerken van bestanden: {e}",
        )
    output_file_week = validation_result["outputFileWeek"]
    output_file_day = validation_result["outputFileDay"]

    if not os.path.exists(output_file_week) or not os.path.exists(output_file_day):
        raise HTTPException(
            status_code=400,
            detail="De verwachte outputbestanden zijn niet gegenereerd door de validatie.",
        )

    email_body = format_validation_email_body(validation_result)

    return {
        "emailBody": email_body,
        "outputFileWeek": output_file_week,
        "outputFileDay": output_file_day,
    }

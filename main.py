from contextlib import asynccontextmanager
import csv
import io
import os
from pathlib import Path
import tempfile
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
WAGEGROUPS_CSV_PATH = (BASE_DIR / "person_wagegroups.csv").resolve()
WAGEGROUPS_HEADERS = ["id", "name", "wagegroup"]


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


def _normalize_person_name(name: str) -> str:
    return " ".join(name.strip().lower().split())


def _normalize_header_cell(value: str) -> str:
    normalized = value.replace("\ufeff", "")
    normalized = re.sub(r"[\s,;:_-]+", " ", normalized.strip().lower())
    return " ".join(normalized.split())


def _is_bulk_header_row(name: str, wagegroup: str) -> bool:
    normalized_name = _normalize_header_cell(name)
    normalized_wagegroup = _normalize_header_cell(wagegroup)

    name_headers = {
        "name",
        "naam",
        "achternaam voornaam",
        "voornaam achternaam",
    }
    wagegroup_headers = {
        "wagegroup",
        "wage group",
        "loon groep",
        "loongroep",
    }

    if normalized_name not in name_headers:
        return False
    if not normalized_wagegroup:
        return True
    return normalized_wagegroup in wagegroup_headers


def _clean_person_fields(name: str, wagegroup: str) -> tuple[str, str]:
    cleaned_name = name.strip()
    cleaned_wagegroup = wagegroup.strip()
    if not cleaned_name or not cleaned_wagegroup:
        raise HTTPException(
            status_code=400,
            detail="Velden 'name' en 'wagegroup' zijn verplicht.",
        )
    return cleaned_name, cleaned_wagegroup


def _ensure_wagegroups_csv() -> None:
    if WAGEGROUPS_CSV_PATH.exists():
        return
    with open(WAGEGROUPS_CSV_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=WAGEGROUPS_HEADERS)
        writer.writeheader()


def _read_wagegroups() -> list[dict[str, str]]:
    _ensure_wagegroups_csv()
    with open(WAGEGROUPS_CSV_PATH, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        missing_headers = [
            header
            for header in WAGEGROUPS_HEADERS
            if header not in (reader.fieldnames or [])
        ]
        if missing_headers:
            raise HTTPException(
                status_code=500,
                detail=(
                    "CSV-indeling voor wagegroups is ongeldig. "
                    f"Ontbrekende kolommen: {', '.join(missing_headers)}"
                ),
            )

        rows: list[dict[str, str]] = []
        for row in reader:
            id_value = str(row.get("id", "")).strip()
            name = str(row.get("name", "")).strip()
            wagegroup = str(row.get("wagegroup", "")).strip()

            if not id_value and not name and not wagegroup:
                continue

            rows.append(
                {
                    "id": id_value,
                    "name": name,
                    "wagegroup": wagegroup,
                }
            )
        return rows


def _write_wagegroups(rows: list[dict[str, str]]) -> None:
    _ensure_wagegroups_csv()
    fd, temp_path = tempfile.mkstemp(
        prefix="person_wagegroups_",
        suffix=".csv",
        dir=str(BASE_DIR),
    )
    os.close(fd)

    try:
        with open(temp_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=WAGEGROUPS_HEADERS)
            writer.writeheader()
            for row in rows:
                writer.writerow(
                    {
                        "id": str(row.get("id", "")).strip(),
                        "name": str(row.get("name", "")).strip(),
                        "wagegroup": str(row.get("wagegroup", "")).strip(),
                    }
                )
        os.replace(temp_path, WAGEGROUPS_CSV_PATH)
    finally:
        if os.path.exists(temp_path):
            os.remove(temp_path)


def _next_person_id(rows: list[dict[str, str]]) -> int:
    max_id = 0
    for row in rows:
        try:
            max_id = max(max_id, int(str(row.get("id", "")).strip()))
        except ValueError:
            continue
    return max_id + 1


def _upsert_person_wagegroup(
    rows: list[dict[str, str]], name: str, wagegroup: str
) -> str:
    normalized_name = _normalize_person_name(name)
    for row in rows:
        if _normalize_person_name(str(row.get("name", ""))) == normalized_name:
            row["name"] = name
            row["wagegroup"] = wagegroup
            return "updated"

    rows.append(
        {
            "id": str(_next_person_id(rows)),
            "name": name,
            "wagegroup": wagegroup,
        }
    )
    return "inserted"


@app.get("/health")
def health():
    return {"status": "ok"}


class DownloadRequest(BaseModel):
    fileName: str


class WagePersonUpdateRequest(BaseModel):
    name: str
    wagegroup: str


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


@app.get("/wagegroups")
async def get_wagegroups(_: None = Depends(verify_api_key)):
    rows = _read_wagegroups()
    people = []
    for row in rows:
        try:
            person_id = int(str(row.get("id", "")).strip())
        except ValueError:
            continue
        people.append(
            {
                "id": person_id,
                "name": str(row.get("name", "")).strip(),
                "wagegroup": str(row.get("wagegroup", "")).strip(),
            }
        )
    return people


@app.post("/update_wage_person")
async def update_wage_person(
    payload: WagePersonUpdateRequest,
    _: None = Depends(verify_api_key),
):
    name, wagegroup = _clean_person_fields(payload.name, payload.wagegroup)
    rows = _read_wagegroups()
    action = _upsert_person_wagegroup(rows, name, wagegroup)
    _write_wagegroups(rows)
    return {"status": "ok", "action": action}


@app.post("/update_wages")
async def update_wages(
    file: UploadFile = File(...),
    _: None = Depends(verify_api_key),
):
    if not file.filename:
        raise HTTPException(status_code=400, detail="Geen bestandsnaam ontvangen.")
    if not file.filename.lower().endswith(".xlsx"):
        raise HTTPException(
            status_code=400, detail="Alleen .xlsx-bestanden worden ondersteund."
        )

    content = await file.read()
    if not content:
        raise HTTPException(status_code=400, detail="Leeg bestand ontvangen.")

    try:
        wb = load_workbook(io.BytesIO(content), data_only=True)
    except Exception as e:
        raise HTTPException(
            status_code=400, detail="Kon Excelbestand niet lezen."
        ) from e

    ws = wb.active
    rows = _read_wagegroups()

    processed = 0
    inserted = 0
    updated = 0
    skipped = 0

    for idx, excel_row in enumerate(ws.iter_rows(values_only=True)):
        raw_name = excel_row[0] if len(excel_row) > 0 else None
        raw_wagegroup = excel_row[1] if len(excel_row) > 1 else None

        name = str(raw_name).strip() if raw_name is not None else ""
        wagegroup = str(raw_wagegroup).strip() if raw_wagegroup is not None else ""

        if _is_bulk_header_row(name, wagegroup):
            skipped += 1
            continue

        if not name and not wagegroup:
            continue

        if not name or not wagegroup:
            skipped += 1
            continue

        processed += 1
        action = _upsert_person_wagegroup(rows, name, wagegroup)
        if action == "inserted":
            inserted += 1
        else:
            updated += 1

    _write_wagegroups(rows)

    return {
        "status": "ok",
        "processed": processed,
        "inserted": inserted,
        "updated": updated,
        "skipped": skipped,
    }


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

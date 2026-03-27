"""
CSV-to-database loaders for each file type.

File type detection is based on filename patterns:
  - 'Kloklijst' + 'Otto Workforce'   → kloklijst (agency=otto)
  - 'Kloklijst' + 'Flexspecialisten' → kloklijst (agency=flexspecialisten)
  - 'Export Factuur'                  → invoice_lines
  - 'Tarievensheet'                   → tarievensheet
  - 'OTTO -Padifood tarievenoverzicht'→ otto_rate_card
"""

import io
import os
import re
from typing import Optional

import pandas as pd
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import AsyncSession

from convert_flex import parse_flex_pdfs
from models import InvoiceLine, Kloklijst, OttoRateCard, Tarievensheet


def _extract_week_number(filename: str) -> Optional[int]:
    """Extract the 6-digit ISO week number from the start of a filename (e.g. 202551)."""
    match = re.match(r"^(\d{6})", filename)
    return int(match.group(1)) if match else None


def _to_float(val) -> Optional[float]:
    try:
        if pd.isna(val):
            return None
        return float(val)
    except (TypeError, ValueError):
        return None


def _to_int(val) -> Optional[int]:
    try:
        if pd.isna(val):
            return None
        return int(float(val))
    except (TypeError, ValueError):
        return None


def _to_date(val):
    if pd.isna(val):
        return None
    ts = pd.to_datetime(val, errors="coerce")
    return ts.date() if not pd.isna(ts) else None


def _to_datetime(val):
    if pd.isna(val):
        return None
    ts = pd.to_datetime(val, errors="coerce")
    return ts.to_pydatetime() if not pd.isna(ts) else None


# ---------------------------------------------------------------------------
# Kloklijst loader
# ---------------------------------------------------------------------------

def _load_kloklijst_df(content: bytes, week_number: int, agency: str) -> list[Kloklijst]:
    df = pd.read_csv(
        io.BytesIO(content),
        encoding="utf-8-sig",
        dtype=str,
        keep_default_na=False,
    )
    df = df.replace("", None)

    # Forward-fill employee identifiers across the block rows
    for col in ["Loonnummers", "Personeelsnummer", "Naam", "Afd."]:
        if col in df.columns:
            df[col] = df[col].ffill()

    rows = []
    for _, r in df.iterrows():
        rows.append(
            Kloklijst(
                week_number=week_number,
                agency=agency,
                loonnummers=_to_int(r.get("Loonnummers")),
                personeelsnummer=_to_int(r.get("Personeelsnummer")),
                naam=r.get("Naam"),
                afdeling=r.get("Afd."),
                datum=_to_date(r.get("Datum")),
                start=_to_datetime(r.get("start")),
                eind=_to_datetime(r.get("Eind")),
                pauze_genomen_dag=_to_float(r.get("Pauze genomen Dag")),
                pauze_afgetrokken_dag=_to_float(r.get("Pauze afgetrokken Dag")),
                pzcor_dag=_to_float(r.get("Pzcor Dag")),
                norm_uren_dag=_to_float(r.get("Norm uren Dag")),
                t133_dag=_to_float(r.get("T133 Dag")),
                t135_dag=_to_float(r.get("T135 Dag")),
                t200_dag=_to_float(r.get("T200 Dag")),
                ow140_week=_to_float(r.get("OW140 Week")),
                ow180_dag=_to_float(r.get("OW180 Dag")),
                ow200_dag=_to_float(r.get("OW200 Dag")),
                effectieve_uren_dag=_to_float(r.get("Effectieve uren Dag")),
            )
        )
    return rows


# ---------------------------------------------------------------------------
# Invoice lines loader (Export Factuur)
# ---------------------------------------------------------------------------

def _load_invoice_lines_df(content: bytes, week_number: int) -> list[InvoiceLine]:
    df = pd.read_csv(
        io.BytesIO(content),
        encoding="utf-8-sig",
        dtype=str,
        keep_default_na=False,
    )
    df = df.replace("", None)

    rows = []
    for _, r in df.iterrows():
        datum = _to_date(r.get("Datum"))
        if datum is None:
            continue  # skip malformed rows
        rows.append(
            InvoiceLine(
                week_number=week_number,
                agency="otto",
                sap_id=r.get("SAP ID", ""),
                naam=r.get("Naam", ""),
                uurloon=_to_float(r.get("Uurloon")) or 0.0,
                uurloon_zonder_atv=_to_float(r.get("Uurloon zonder ATV")) or 0.0,
                functie_toeslag=_to_float(r.get("Functie toeslag")) or 0.0,
                wekentelling=_to_int(r.get("Wekentelling")) or 0,
                fase_tarief=r.get("Fase tarief", ""),
                datum=datum,
                code_toeslag=r.get("Code toeslag", ""),
                totaal_uren=_to_float(r.get("Totaal uren")) or 0.0,
                subtotaal=_to_float(r.get("Subtotaal")) or 0.0,
            )
        )
    return rows


# ---------------------------------------------------------------------------
# Tarievensheet loader
# ---------------------------------------------------------------------------

def _load_tarievensheet_df(content: bytes, week_number: int) -> list[Tarievensheet]:
    # The tarievensheet has duplicate column headers, so we read by position.
    df = pd.read_csv(
        io.BytesIO(content),
        encoding="utf-8-sig",
        dtype=str,
        keep_default_na=False,
        header=0,
    )
    df = df.replace("", None)

    # Positional column indices (0-based after pandas reads the header row):
    # 0: SAP ID, 1: Naam, 2: Uurloon, 3: Uurloon zonder ATV, 4: Functie toeslag
    # 5: Wekentelling, 6: Fase tarief, 7: Datum, 8: Code toeslag
    # 9: Som van Totaal uren, 10: Som van Subtotaal
    # 11: Tarief, 12: (empty), 13: orf, 14: TF, 15: OF, 16: ATV, 17: ATV, 18: marge
    # 19: 1, 20: 1.33, 21: 1.35, 22: 1.8 (day), 23: 2, 24: 3
    # 25: 1.4, 26: 1.8 (OW), 27: 2 (OW), 28: 3 (OW)
    # 29: (empty), 30: Fase tarief (actual), 31: ORF (actual)

    def col(idx):
        cols = df.columns.tolist()
        return cols[idx] if idx < len(cols) else None

    rows = []
    for _, r in df.iterrows():
        datum = _to_date(r.iloc[7] if len(r) > 7 else None)
        if datum is None:
            continue
        rows.append(
            Tarievensheet(
                week_number=week_number,
                sap_id=str(r.iloc[0]) if r.iloc[0] is not None else "",
                naam=str(r.iloc[1]) if r.iloc[1] is not None else "",
                uurloon=_to_float(r.iloc[2]) or 0.0,
                uurloon_zonder_atv=_to_float(r.iloc[3]) or 0.0,
                functie_toeslag=_to_float(r.iloc[4]) or 0.0,
                wekentelling=_to_int(r.iloc[5]) or 0,
                fase_tarief=str(r.iloc[6]) if r.iloc[6] is not None else "",
                datum=datum,
                code_toeslag=str(r.iloc[8]) if len(r) > 8 and r.iloc[8] is not None else "",
                som_totaal_uren=_to_float(r.iloc[9]) if len(r) > 9 else 0.0,
                som_subtotaal=_to_float(r.iloc[10]) if len(r) > 10 else 0.0,
                tarief=_to_float(r.iloc[11]) if len(r) > 11 else None,
                orf=_to_float(r.iloc[13]) if len(r) > 13 else None,
                marge=_to_float(r.iloc[18]) if len(r) > 18 else None,
                rate_norm=_to_float(r.iloc[19]) if len(r) > 19 else None,
                rate_133=_to_float(r.iloc[20]) if len(r) > 20 else None,
                rate_135=_to_float(r.iloc[21]) if len(r) > 21 else None,
                rate_180_day=_to_float(r.iloc[22]) if len(r) > 22 else None,
                rate_200=_to_float(r.iloc[23]) if len(r) > 23 else None,
                rate_300=_to_float(r.iloc[24]) if len(r) > 24 else None,
                rate_140=_to_float(r.iloc[25]) if len(r) > 25 else None,
                rate_180_ow=_to_float(r.iloc[26]) if len(r) > 26 else None,
                rate_200_ow=_to_float(r.iloc[27]) if len(r) > 27 else None,
                rate_300_ow=_to_float(r.iloc[28]) if len(r) > 28 else None,
                fase_actual=str(r.iloc[30]) if len(r) > 30 and r.iloc[30] is not None else None,
                orf_actual=_to_float(r.iloc[31]) if len(r) > 31 else None,
            )
        )
    return rows


# ---------------------------------------------------------------------------
# OTTO rate card loader
# ---------------------------------------------------------------------------

def _load_otto_rate_card_df(content: bytes) -> list[OttoRateCard]:
    df = pd.read_csv(
        io.BytesIO(content),
        encoding="utf-8-sig",
        dtype=str,
        keep_default_na=False,
    )
    df = df.replace("", None)

    # Columns (positional, duplicate headers for rate columns):
    # 0: Personeelsnummer, 1: Achternaam, 2: Voornaam, 3: Wekenteller, 4: Schaal
    # 5: Uurloon incl. ATV, 6: Uurloon excl. ATV
    # 7: 1, 8: 1.33, 9: 1.35, 10: 1.8 (day), 11: 2
    # 12: 1.4, 13: 1.8 (OW), 14: 2 (OW)

    rows = []
    for _, r in df.iterrows():
        # Skip rows without a last name (empty/footer rows)
        if r.iloc[1] is None:
            continue
        rows.append(
            OttoRateCard(
                personeelsnummer=_to_int(r.iloc[0]),
                achternaam=str(r.iloc[1]),
                voornaam=str(r.iloc[2]) if r.iloc[2] is not None else "",
                wekenteller=_to_int(r.iloc[3]),
                schaal=str(r.iloc[4]) if len(r) > 4 and r.iloc[4] is not None else None,
                uurloon_incl_atv=_to_float(r.iloc[5]) or 0.0,
                uurloon_excl_atv=_to_float(r.iloc[6]) or 0.0,
                rate_norm=_to_float(r.iloc[7]) if len(r) > 7 else None,
                rate_133=_to_float(r.iloc[8]) if len(r) > 8 else None,
                rate_135=_to_float(r.iloc[9]) if len(r) > 9 else None,
                rate_180_day=_to_float(r.iloc[10]) if len(r) > 10 else None,
                rate_200=_to_float(r.iloc[11]) if len(r) > 11 else None,
                rate_140=_to_float(r.iloc[12]) if len(r) > 12 else None,
                rate_180_ow=_to_float(r.iloc[13]) if len(r) > 13 else None,
                rate_200_ow=_to_float(r.iloc[14]) if len(r) > 14 else None,
            )
        )
    return rows


# ---------------------------------------------------------------------------
# Flexspecialisten invoice loader (PDF)
# ---------------------------------------------------------------------------

async def load_flex_invoices(
    pdfs: list[tuple[str, bytes]],
    week_number: int,
    session: AsyncSession,
) -> dict:
    """
    Parse one or more Flexspecialisten PDF invoices for the same week,
    merge them (corrections override originals), and insert into invoice_lines.

    Args:
        pdfs: list of (filename, raw_bytes) tuples
        week_number: ISO week number (YYYYww)
        session: async DB session
    """
    pdf_contents = [content for _, content in pdfs]
    rows_data = parse_flex_pdfs(pdf_contents)

    rows = [
        InvoiceLine(
            week_number=week_number,
            agency="flexspecialisten",
            sap_id="",
            naam=r["naam"],
            uurloon=r["tarief"],
            uurloon_zonder_atv=0.0,
            functie_toeslag=0.0,
            wekentelling=0,
            fase_tarief="",
            datum=None,          # PDF invoices have weekly totals only
            code_toeslag=r["code_toeslag"],
            totaal_uren=r["totaal_uren"],
            subtotaal=r["subtotaal"],
        )
        for r in rows_data
    ]

    session.add_all(rows)
    await session.commit()
    return {
        "table": "invoice_lines",
        "agency": "flexspecialisten",
        "week": week_number,
        "rows": len(rows),
        "pdfs": [fname for fname, _ in pdfs],
    }


# ---------------------------------------------------------------------------
# Public dispatcher
# ---------------------------------------------------------------------------

async def load_file(filename: str, content: bytes, session: AsyncSession) -> dict:
    """
    Detect file type from filename, parse the CSV content, and insert rows
    into the appropriate table. Returns a summary dict.
    """
    name = filename
    name_lower = filename.lower()

    if "kloklijst" in name_lower:
        week_number = _extract_week_number(name)
        if week_number is None:
            return {"skipped": True, "reason": "Could not extract week number"}

        if "otto" in name_lower:
            agency = "otto"
        elif "flex" in name_lower:
            agency = "flexspecialisten"
        else:
            return {"skipped": True, "reason": "Unknown kloklijst agency"}

        rows = _load_kloklijst_df(content, week_number, agency)
        await session.execute(
            delete(Kloklijst).where(
                Kloklijst.week_number == week_number,
                Kloklijst.agency == agency,
            )
        )
        session.add_all(rows)
        await session.commit()
        return {"table": "kloklijst", "week": week_number, "agency": agency, "rows": len(rows)}

    elif "export factuur" in name_lower:
        week_number = _extract_week_number(name)
        if week_number is None:
            return {"skipped": True, "reason": "Could not extract week number"}

        rows = _load_invoice_lines_df(content, week_number)
        await session.execute(
            delete(InvoiceLine).where(InvoiceLine.week_number == week_number)
        )
        session.add_all(rows)
        await session.commit()
        return {"table": "invoice_lines", "week": week_number, "rows": len(rows)}

    elif "tarievensheet" in name_lower:
        week_number = _extract_week_number(name)
        if week_number is None:
            return {"skipped": True, "reason": "Could not extract week number"}

        rows = _load_tarievensheet_df(content, week_number)
        await session.execute(
            delete(Tarievensheet).where(Tarievensheet.week_number == week_number)
        )
        session.add_all(rows)
        await session.commit()
        return {"table": "tarievensheet", "week": week_number, "rows": len(rows)}

    elif "tarievenoverzicht" in name_lower:
        rows = _load_otto_rate_card_df(content)
        session.add_all(rows)
        await session.commit()
        return {"table": "otto_rate_card", "rows": len(rows)}

    else:
        return {"skipped": True, "reason": f"Unrecognised file type: {filename}"}

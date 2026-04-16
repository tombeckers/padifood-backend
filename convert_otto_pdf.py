"""
OTTO Workforce PDF invoice parser.

Parses the "Gewerkte uren" table from an OTTO invoice PDF (image-based, requires OCR).
Pages before the "Gewerkte uren" header are skipped automatically.

Returns a flat list of per-employee, per-hour-type rows ready for DB insertion,
in the same shape as convert_flex.parse_flex_pdfs().

Uursoort → code_toeslag mapping:
  normale uren ma-vr  100%  → Norm uren Dag
  normale uren ma-vr  133%  → T133 Dag
  normale uren ma-vr  135%  → T135 Dag
  normale uren ma-vr  140%  → OW140 Week
  overwerk ma-vr      140%  → OW140 Week
  normale uren ma-vr  180%  → OW180 Dag
  normale uren ma-vr  200%  → OW200 Dag
"""

from __future__ import annotations

import io
import re
from collections import defaultdict
from typing import Optional

import fitz  # pymupdf
import numpy as np
from rapidocr_onnxruntime import RapidOCR

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DPI = 200

# X-coordinate boundaries (at DPI=200) that separate columns.
# Derived from bounding-box inspection of factuur week 9 OTTO.pdf.
#   col_name  : x0 < 500
#   col_uren  : 500 <= x0 < 690
#   col_omsch : 690 <= x0 < 1000
#   col_pct   : 1000 <= x0 < 1160
#   col_tarief: 1160 <= x0 < 1350
#   col_bedrag: x0 >= 1350
COL_UREN_X   = 500
COL_OMSCH_X  = 680   # omschrijving starts at x0~688; keep margin below that
COL_PCT_X    = 1000
COL_TARIEF_X = 1160
COL_BEDRAG_X = 1350

# Row-grouping tolerance in pixels
ROW_Y_TOLERANCE = 18

# Person number: 6-digit number at the start of a name cell
PERSNR_RE = re.compile(r"^(\d{5,7})\s*(.+)$")

# Percentage extraction: "133,00 %" → "133"
PCT_RE = re.compile(r"(\d+)[,.]?\d*\s*%")

UURSOORT_MAP: dict[tuple[str, str], str] = {
    ("normale uren ma-vr", "100"): "Norm uren Dag",
    ("normale uren ma-vr", "133"): "T133 Dag",
    ("normale uren ma-vr", "135"): "T135 Dag",
    ("normale uren ma-vr", "140"): "OW140 Week",
    ("overwerk ma-vr",     "140"): "OW140 Week",
    ("overwerk za",        "140"): "OW140 Week",
    ("normale uren ma-vr", "180"): "OW180 Dag",
    ("overwerk ma-vr",     "180"): "OW180 Dag",
    ("overwerk za",        "180"): "OW180 Dag",
    ("normale uren ma-vr", "200"): "OW200 Dag",
    ("overwerk ma-vr",     "200"): "OW200 Dag",
    ("overwerk za",        "200"): "OW200 Dag",
    ("overwerk zo",        "180"): "OW180 Dag",
    ("overwerk zo",        "200"): "OW200 Dag",
}

# Metadata regexes (page headers, first detail page)
FACTUURNR_RE   = re.compile(r"Factuurnummer[:\s]+(\S+)", re.IGNORECASE)
FACTUURDATUM_RE = re.compile(r"Factuurdatum[:\s]+(\d{2}[.\-/]\d{2}[.\-/]\d{4})", re.IGNORECASE)
WEEK_RE        = re.compile(r"Geleverde\s*arbeid\s*week[:\s]+(\d{2})(\d{4})", re.IGNORECASE)

# ---------------------------------------------------------------------------
# OCR engine (module-level singleton — loaded once per process)
# ---------------------------------------------------------------------------

_ocr: Optional[RapidOCR] = None


def _get_ocr() -> RapidOCR:
    global _ocr
    if _ocr is None:
        _ocr = RapidOCR()
    return _ocr


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse_dutch_float(s: str) -> float:
    """Convert Dutch number format: '1.257,29' → 1257.29, '39,75' → 39.75."""
    return float(s.replace(".", "").replace(",", "."))


_OMSCH_FIXES = [
    # OCR joins words without spaces — restore canonical spacing
    (re.compile(r"normaleurenma-vr",    re.IGNORECASE), "normale uren ma-vr"),
    (re.compile(r"normale\s*urenma-vr", re.IGNORECASE), "normale uren ma-vr"),
    (re.compile(r"overwerkma-vr",       re.IGNORECASE), "overwerk ma-vr"),
    (re.compile(r"overwerkza\b",        re.IGNORECASE), "overwerk za"),
    (re.compile(r"overwerkzo\b",        re.IGNORECASE), "overwerk zo"),
]


def _normalize_omschrijving(s: str) -> str:
    """Normalise OCR noise in hour-type strings."""
    s = re.sub(r"\s+", " ", s.strip().lower())
    for pattern, replacement in _OMSCH_FIXES:
        s = pattern.sub(replacement, s)
    return s


def _extract_pct(s: str) -> Optional[str]:
    m = PCT_RE.search(s)
    return m.group(1) if m else None


def _render_page(page: fitz.Page) -> np.ndarray:
    mat = fitz.Matrix(DPI / 72, DPI / 72)
    pix = page.get_pixmap(matrix=mat, colorspace=fitz.csRGB)
    return np.frombuffer(pix.samples, dtype=np.uint8).reshape(pix.height, pix.width, 3)


def _ocr_items(page: fitz.Page) -> list[dict]:
    """Run OCR on a page and return items with text + bounding box."""
    img = _render_page(page)
    result, _ = _get_ocr()(img)
    if not result:
        return []
    items = []
    for bbox, text, conf in result:
        xs = [p[0] for p in bbox]
        ys = [p[1] for p in bbox]
        items.append({
            "text": text.strip(),
            "conf": conf,
            "x0": min(xs),
            "y0": min(ys),
            "x1": max(xs),
            "y1": max(ys),
        })
    return items


def _assign_column(x0: float) -> str:
    if x0 < COL_UREN_X:
        return "name"
    if x0 < COL_OMSCH_X:
        return "uren"
    if x0 < COL_PCT_X:
        return "omschrijving"
    if x0 < COL_TARIEF_X:
        return "percentage"
    if x0 < COL_BEDRAG_X:
        return "tarief"
    return "bedrag"


def _group_rows(items: list[dict]) -> list[dict[str, str]]:
    """
    Group OCR items into logical table rows by y-coordinate proximity.
    Returns list of {col: text} dicts, sorted top-to-bottom.
    """
    if not items:
        return []

    # Sort by y0, then x0
    sorted_items = sorted(items, key=lambda i: (i["y0"], i["x0"]))

    rows: list[list[dict]] = []
    current_row: list[dict] = [sorted_items[0]]

    for item in sorted_items[1:]:
        last_y = sum(i["y0"] for i in current_row) / len(current_row)
        if abs(item["y0"] - last_y) <= ROW_Y_TOLERANCE:
            current_row.append(item)
        else:
            rows.append(current_row)
            current_row = [item]
    rows.append(current_row)

    result = []
    for row_items in rows:
        cell: dict[str, list[str]] = defaultdict(list)
        for it in sorted(row_items, key=lambda i: i["x0"]):
            col = _assign_column(it["x0"])
            cell[col].append(it["text"])
        result.append({col: " ".join(texts) for col, texts in cell.items()})

    return result


def _parse_person_cell(text: str) -> tuple[Optional[str], Optional[str]]:
    """
    Extract (persnummer, naam) from a name-column cell.
    OCR sometimes splits the number and name across two detections;
    by the time we call this they are joined with a space.
    Returns (None, None) if the cell looks like a function/role header.
    """
    m = PERSNR_RE.match(text.strip())
    if not m:
        return None, None
    persnummer = m.group(1)
    naam_raw = m.group(2).strip().strip(",").strip()
    # Normalise "Lastname,Firstname" → "Lastname Firstname"
    naam = naam_raw.replace(",", " ").strip()
    naam = re.sub(r"\s+", " ", naam)
    # Strip stray leading digits/non-letter chars from OCR split artifacts (e.g. "5Jankowski")
    naam = re.sub(r"^[\d\W]+", "", naam).strip()
    return persnummer, naam


# ---------------------------------------------------------------------------
# Single PDF parser
# ---------------------------------------------------------------------------

def _parse_single_pdf(content: bytes) -> dict:
    """
    Parse one OTTO invoice PDF.

    Returns:
        {
            'factuurnr':    str | None,
            'factuurdatum': str | None,
            'week':         int | None,
            'employees':    {persnummer: {'naam': str,
                                          code_toeslag: {'hours': float,
                                                         'tarief': float,
                                                         'subtotaal': float}}}
        }
    """
    result: dict = {
        "factuurnr": None,
        "factuurdatum": None,
        "week": None,
        "employees": {},
    }

    in_gewerkte_uren = False

    with fitz.open(stream=content, filetype="pdf") as doc:
        ocr = _get_ocr()

        for page in doc:
            img = _render_page(page)
            raw_result, _ = ocr(img)
            if not raw_result:
                continue

            items = []
            full_text_parts = []
            for bbox, text, conf in raw_result:
                text = text.strip()
                if not text:
                    continue
                full_text_parts.append(text)
                xs = [p[0] for p in bbox]
                ys = [p[1] for p in bbox]
                items.append({
                    "text": text,
                    "conf": conf,
                    "x0": min(xs),
                    "y0": min(ys),
                    "x1": max(xs),
                    "y1": max(ys),
                })

            full_text = " ".join(full_text_parts)

            # Extract metadata from any page (headers repeat on detail pages)
            if result["factuurnr"] is None:
                m = FACTUURNR_RE.search(full_text)
                if m:
                    result["factuurnr"] = m.group(1)

            if result["factuurdatum"] is None:
                m = FACTUURDATUM_RE.search(full_text)
                if m:
                    result["factuurdatum"] = m.group(1)

            if result["week"] is None:
                m = WEEK_RE.search(full_text)
                if m:
                    # Store as 6-digit YYYYWW to match kloklijst filename convention
                    result["week"] = int(m.group(2)) * 100 + int(m.group(1))

            # Detect start of "Gewerkte uren" section
            if not in_gewerkte_uren:
                if re.search(r"gewerkte\s+uren", full_text, re.IGNORECASE):
                    in_gewerkte_uren = True
                else:
                    continue

            # Parse table rows
            rows = _group_rows(items)

            current_persnummer: Optional[str] = None
            current_naam: Optional[str] = None

            for row in rows:
                name_cell = row.get("name", "").strip()
                uren_cell = row.get("uren", "").strip()
                omsch_cell = _normalize_omschrijving(row.get("omschrijving", ""))
                pct_cell = row.get("percentage", "").strip()
                tarief_cell = row.get("tarief", "").strip()
                bedrag_cell = row.get("bedrag", "").strip()

                # Try to parse as a person row
                if name_cell:
                    persnummer, naam = _parse_person_cell(name_cell)
                    if persnummer:
                        current_persnummer = persnummer
                        current_naam = naam
                        if persnummer not in result["employees"]:
                            result["employees"][persnummer] = {
                                "naam": naam,
                            }

                # Data row: needs hours + omschrijving + percentage + bedrag
                if not (uren_cell and omsch_cell and pct_cell and bedrag_cell):
                    continue
                if current_persnummer is None:
                    continue

                pct = _extract_pct(pct_cell)
                if pct is None:
                    continue

                try:
                    hours = _parse_dutch_float(uren_cell)
                    subtotaal = _parse_dutch_float(bedrag_cell)
                    tarief = _parse_dutch_float(tarief_cell) if tarief_cell else 0.0
                except ValueError:
                    continue

                code_toeslag = UURSOORT_MAP.get((omsch_cell, pct))
                if code_toeslag is None:
                    code_toeslag = f"{omsch_cell} {pct}%"

                emp = result["employees"][current_persnummer]
                if code_toeslag not in emp:
                    emp[code_toeslag] = {"hours": 0.0, "tarief": 0.0, "subtotaal": 0.0}
                emp[code_toeslag]["hours"] += hours
                emp[code_toeslag]["tarief"] = tarief
                emp[code_toeslag]["subtotaal"] += subtotaal

    return result


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def parse_otto_pdfs(pdf_contents: list[bytes]) -> list[dict]:
    """
    Parse one or more OTTO invoice PDFs for the same week and return a
    merged, flat list of invoice rows.

    Correction handling: a later-processed PDF replaces all lines for any
    employee it contains (same logic as convert_flex.parse_flex_pdfs).

    Returns a list of dicts:
        {
            'sap_id':       str,
            'naam':         str,
            'code_toeslag': str,
            'totaal_uren':  float,
            'tarief':       float,
            'subtotaal':    float,
        }
    """
    if not pdf_contents:
        return []

    invoices = [_parse_single_pdf(content) for content in pdf_contents]

    # Merge: later invoice replaces all lines for any employee it covers
    merged: dict[str, dict] = {}
    for invoice in invoices:
        for persnummer, emp_data in invoice["employees"].items():
            merged[persnummer] = dict(emp_data)

    rows = []
    for persnummer, emp_data in merged.items():
        naam = emp_data.get("naam", "")
        for code_toeslag, data in emp_data.items():
            if code_toeslag == "naam":
                continue
            rows.append({
                "sap_id": persnummer,
                "naam": naam,
                "code_toeslag": code_toeslag,
                "totaal_uren": round(data["hours"], 4),
                "tarief": data["tarief"],
                "subtotaal": round(data["subtotaal"], 4),
            })

    return rows


def extract_week_from_otto_pdfs(pdf_contents: list[bytes]) -> Optional[int]:
    """Extract the week number from the first successfully parsed PDF."""
    for content in pdf_contents:
        result = _parse_single_pdf(content)
        if result["week"] is not None:
            return result["week"]
    return None

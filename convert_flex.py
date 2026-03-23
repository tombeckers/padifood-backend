"""
Flexspecialisten PDF invoice parser.

Parses one or more Flexspecialisten invoice PDFs for the same week and returns
a merged list of per-employee, per-hour-type rows ready for DB insertion.

Correction handling (option b):
  When multiple PDFs cover the same week, the one with the latest Factuurdatum
  wins for any employee that appears in both. This means a correction invoice
  automatically replaces the original data for the affected employees while
  keeping unaffected employees from the original.

Uursoort → code_toeslag mapping (matches kloklijst column names):
  Normale uren  100% → Norm uren Dag
  Toeslag uren  133% → T133 Dag
  Toeslag uren  135% → T135 Dag
  Overuren      140% → OW140 Week
  Overuren      180% → OW180 Dag
  Overuren      200% → OW200 Dag
"""

import io
import re
from collections import defaultdict
from datetime import date, datetime
from typing import Optional

import pdfplumber


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

UURSOORT_MAP: dict[tuple[str, str], str] = {
    ("Normale uren", "100,00"): "Norm uren Dag",
    ("Toeslag uren", "133,00"): "T133 Dag",
    ("Toeslag uren", "135,00"): "T135 Dag",
    ("Overuren",     "140,00"): "OW140 Week",
    ("Overuren",     "180,00"): "OW180 Dag",
    ("Overuren",     "200,00"): "OW200 Dag",
}

TITLE_RE = re.compile(r"^(De heer|Mevrouw)\s+", re.IGNORECASE)

# Data line pattern:
#   Normale uren 202602 100,00% € 31,63 39,75 € 1.257,29
# The currency symbol is often a replacement char (\ufffd) due to PDF encoding.
DATA_LINE_RE = re.compile(
    r"^(Normale uren|Toeslag uren|Overuren)\s+"
    r"(\d{6})\s+"           # week number
    r"(\d+,\d+)%\s+"        # percentage like "133,00%"
    r"\S+\s+"               # currency symbol
    r"([\d.,]+)\s+"         # tarief (billing rate)
    r"([\d.,]+)\s+"         # aantal uren
    r"\S+\s+"               # currency symbol
    r"([\d.,]+)$"           # totaal bedrag
)

FACTUURNR_RE = re.compile(r"^Factuurnr\.\s+(\S+)")
FACTUURDATUM_RE = re.compile(r"Factuurdatum\s+(\d{2}/\d{2}/\d{4})")

# Lines to skip (page headers, footers, separators)
_SKIP_RES = [re.compile(p, re.IGNORECASE) for p in [
    r"^Padifood Service",
    r"^Factuurnr\.",
    r"^Havenstraat",
    r"^\d{4}\s+[A-Z]{2}",       # postal code line
    r"^Pagina\s+\d",
    r"^#_##",
    r"^Debnr:",
    r"^F\s+A\s+C\s+T\s+U\s+U\s+R",
    r"^Pag\s+\d",
    r"^Uursoort\s+Week",
    r"^Totaal\s+uren",
    r"^FF",                      # doubled footer artifact
    r"^PP",
    r"^RR",
]]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse_dutch_float(s: str) -> float:
    """Convert Dutch number format: '1.257,29' → 1257.29, '39,75' → 39.75."""
    return float(s.replace(".", "").replace(",", "."))


def _parse_invoice_date(date_str: str) -> Optional[date]:
    try:
        return datetime.strptime(date_str.strip(), "%d/%m/%Y").date()
    except ValueError:
        return None


def _should_skip(line: str) -> bool:
    stripped = line.strip()
    if not stripped:
        return True
    for pat in _SKIP_RES:
        if pat.match(stripped):
            return True
    return False


# ---------------------------------------------------------------------------
# Single PDF parser
# ---------------------------------------------------------------------------

def _parse_single_pdf(content: bytes) -> dict:
    """
    Parse one Flexspecialisten PDF.

    Returns:
        {
            'factuurnr':   str,
            'factuurdatum': date | None,
            'week':        int | None,
            'employees':   {naam: {code_toeslag: {'hours': float, 'tarief': float, 'subtotaal': float}}}
        }
    """
    result: dict = {
        "factuurnr": None,
        "factuurdatum": None,
        "week": None,
        "employees": defaultdict(lambda: defaultdict(lambda: {"hours": 0.0, "tarief": 0.0, "subtotaal": 0.0})),
    }

    current_name: Optional[str] = None

    with pdfplumber.open(io.BytesIO(content)) as pdf:
        for page in pdf.pages:
            raw_text = page.extract_text() or ""
            for line in raw_text.split("\n"):
                line = line.strip()

                # Extract invoice metadata from header (first page)
                if result["factuurnr"] is None:
                    m = FACTUURNR_RE.match(line)
                    if m:
                        result["factuurnr"] = m.group(1)
                        continue

                if result["factuurdatum"] is None:
                    m = FACTUURDATUM_RE.search(line)
                    if m:
                        result["factuurdatum"] = _parse_invoice_date(m.group(1))
                        continue

                if _should_skip(line):
                    continue

                # Employee name line
                if TITLE_RE.match(line):
                    current_name = TITLE_RE.sub("", line).strip()
                    continue

                # Hour data line
                if current_name is not None:
                    m = DATA_LINE_RE.match(line)
                    if m:
                        uursoort = m.group(1)
                        week_str = m.group(2)
                        pct_str = m.group(3)    # e.g. "133,00"
                        tarief = _parse_dutch_float(m.group(4))
                        aantal = _parse_dutch_float(m.group(5))
                        subtotaal = _parse_dutch_float(m.group(6))

                        if result["week"] is None:
                            result["week"] = int(week_str)

                        code_toeslag = UURSOORT_MAP.get((uursoort, pct_str))
                        if code_toeslag is None:
                            # Unknown combination — store as-is for visibility
                            code_toeslag = f"{uursoort} {pct_str}%"

                        entry = result["employees"][current_name][code_toeslag]
                        entry["hours"] += aantal
                        entry["tarief"] = tarief        # keep last seen (same per employee/type)
                        entry["subtotaal"] += subtotaal

    return result


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def parse_flex_pdfs(pdf_contents: list[bytes]) -> list[dict]:
    """
    Parse one or more Flexspecialisten PDFs for the same week and return a
    merged, flat list of invoice rows.

    Correction handling:
      PDFs are processed in ascending Factuurdatum order. A later-dated PDF
      replaces ALL hour lines for any employee it contains (the correction
      supersedes the original entry for that employee entirely).

    Returns a list of dicts:
        {
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

    # Sort oldest → newest so later entries override earlier ones
    invoices.sort(key=lambda x: x["factuurdatum"] or date.min)

    # Merge: later invoice replaces all lines for any employee it covers
    merged: dict[str, dict[str, dict]] = {}
    for invoice in invoices:
        for naam, lines in invoice["employees"].items():
            merged[naam] = dict(lines)  # replace entire employee entry

    # Flatten to row list
    rows = []
    for naam, lines in merged.items():
        for code_toeslag, data in lines.items():
            rows.append({
                "naam": naam,
                "code_toeslag": code_toeslag,
                "totaal_uren": round(data["hours"], 4),
                "tarief": data["tarief"],
                "subtotaal": round(data["subtotaal"], 4),
            })

    return rows


def extract_week_from_flex_pdfs(pdf_contents: list[bytes]) -> Optional[int]:
    """Extract the week number from the first successfully parsed PDF."""
    for content in pdf_contents:
        result = _parse_single_pdf(content)
        if result["week"] is not None:
            return result["week"]
    return None

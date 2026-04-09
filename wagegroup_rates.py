from __future__ import annotations

import csv
import io
import os
import re
from collections import Counter, defaultdict
from dataclasses import dataclass
from typing import Any

try:
    from openpyxl import load_workbook
except ImportError:  # pragma: no cover - runtime dependency
    load_workbook = None
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from models import InvoiceLine, PersonWagegroup, PersonWagegroupRate, WagegroupRateCard
from validation_wagegroups import normalize_person_name

def _norm_text(value: Any) -> str:
    if value is None:
        return ""
    return " ".join(str(value).strip().split())


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    text = str(value).strip().replace(",", ".")
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _to_person_number(value: Any) -> str | None:
    text = _norm_text(value)
    if not text:
        return None
    if text.endswith(".0"):
        text = text[:-2]
    return text


def _rate_key_from_header(value: Any) -> str | None:
    text = _norm_text(value).lower()
    if not text:
        return None
    text = text.replace("%", "")
    if text in {"1", "1.0", "100"}:
        return "100"
    if text in {"1.3", "130"}:
        return "130"
    if text in {"1.33", "133"}:
        return "133"
    if text in {"1.35", "135"}:
        return "135"
    if text in {"1.4", "140"}:
        return "140"
    if text in {"1.8", "180"}:
        return "180"
    if text in {"2", "2.0", "200"}:
        return "200"
    if text in {"3", "3.0", "300"}:
        return "300"
    return None


def _rate_key_from_code_toeslag(code_toeslag: str) -> str:
    text = code_toeslag.lower()
    if "133" in text or "1.33" in text:
        return "133"
    if "135" in text or "1.35" in text:
        return "135"
    if "140" in text or "1.4" in text:
        return "140"
    if "180" in text or "1.8" in text:
        return "180"
    if "200" in text or "2.0" in text:
        return "200"
    if "300" in text or "3.0" in text:
        return "300"
    if "130" in text or "1.3" in text:
        return "130"
    return "100"


def _extract_schaal_tarief(text: str) -> tuple[str | None, str | None]:
    clean = _norm_text(text)
    if not clean:
        return None, None
    m = re.search(r"\b([A-Z]\d{1,2})\b", clean.upper())
    schaal = m.group(1) if m else None
    t = re.search(r"\bfase\s+([A-Z])\b", clean, flags=re.IGNORECASE)
    tarief = t.group(1).upper() if t else None
    return schaal, tarief


def _extract_phase(text: str) -> str | None:
    clean = _norm_text(text)
    if not clean:
        return None
    m = re.search(r"\bfase\s+([A-Z])\b", clean, flags=re.IGNORECASE)
    if m:
        return m.group(1).upper()
    if re.fullmatch(r"[A-Z]", clean.upper()):
        return clean.upper()
    return None


@dataclass
class ParsedRateWorkbook:
    person_rates: list[dict[str, Any]]
    rate_card: list[dict[str, Any]]


def parse_otto_wagegroup_rate_workbook(
    content: bytes, filename: str
) -> ParsedRateWorkbook:
    if load_workbook is None:
        raise ValueError("openpyxl is niet beschikbaar in deze omgeving.")
    wb = load_workbook(io.BytesIO(content), data_only=True)
    if "Blad1" not in wb.sheetnames:
        raise ValueError("OTTO tarievenbestand moet sheet 'Blad1' bevatten.")
    ws = wb["Blad1"]

    header = [(_norm_text(c.value)) for c in ws[1]]
    by_name: dict[str, int] = {}
    for i, h in enumerate(header):
        if not h:
            continue
        key = h.lower()
        if key not in by_name:
            by_name[key] = i
    person_idx = by_name.get("personeelsnummer")
    first_name_idx = by_name.get("voornaam")
    last_name_idx = by_name.get("achternaam")
    tarief_idx = by_name.get("fase tarief")
    schaal_idx = by_name.get("schaal")
    if tarief_idx is None:
        tarief_idx = 4  # E
    if schaal_idx is None:
        schaal_idx = 5  # F
    if person_idx is None:
        raise ValueError("Kolom 'Personeelsnummer' is verplicht.")
    if first_name_idx is None or last_name_idx is None:
        raise ValueError("Kolommen 'Voornaam' en 'Achternaam' zijn verplicht.")

    rate_columns = [
        ("100%", "100"),
        ("133%", "133"),
        ("135%", "135"),
        ("180%", "180"),
        ("200%", "200"),
        ("300%", "300"),
    ]
    # Fallback positions for OTTO sheet layout when headers are missing:
    # Q,R,S,T,U,V -> 100,133,135,180,200,300
    fallback_rate_indexes = {
        "100": 16,  # Q
        "133": 17,  # R
        "135": 18,  # S
        "180": 19,  # T
        "200": 20,  # U
        "300": 21,  # V
    }
    rate_indexes: list[tuple[int, str]] = []
    for col_name, rate_key in rate_columns:
        idx = by_name.get(col_name)
        if idx is None:
            idx = fallback_rate_indexes[rate_key]
        rate_indexes.append((idx, rate_key))
    if not rate_indexes:
        raise ValueError(
            "Geen OTTO tariefkolommen gevonden (verwacht 100%/133%/135%/180%/200%/300%)."
        )

    person_rates: list[dict[str, Any]] = []
    for row in ws.iter_rows(min_row=2, values_only=True):
        try:
            person_number = _to_person_number(
                row[person_idx] if person_idx < len(row) else None
            )
            if not person_number:
                continue
            first_name = _norm_text(
                row[first_name_idx] if first_name_idx < len(row) else None
            )
            last_name = _norm_text(
                row[last_name_idx] if last_name_idx < len(row) else None
            )
            name = _norm_text(f"{first_name} {last_name}")
            tarief = _extract_phase(row[tarief_idx] if tarief_idx < len(row) else None)
            schaal = (
                _norm_text(row[schaal_idx] if schaal_idx < len(row) else None) or None
            )
            for idx, rate_key in rate_indexes:
                rate_value = _to_float(row[idx] if idx < len(row) else None)
                if rate_value is None:
                    continue
                person_rates.append(
                    {
                        "provider": "otto",
                        "person_number": person_number,
                        "name": name or person_number,
                        "normalized_name": normalize_person_name(name or person_number),
                        "schaal": schaal,
                        "tarief": tarief,
                        "rate_key": rate_key,
                        "rate_value": rate_value,
                        "source_file": filename,
                        "source_week": None,
                    }
                )
        except Exception:
            continue

    # derive card as median-ish mode by (schaal, tarief, key)
    grouped: dict[tuple[str, str, str], list[float]] = defaultdict(list)
    for row in person_rates:
        schaal = row.get("schaal") or ""
        tarief = row.get("tarief") or ""
        grouped[(str(schaal), str(tarief), str(row["rate_key"]))].append(
            float(row["rate_value"])
        )
    rate_card: list[dict[str, Any]] = []
    for (schaal, tarief, rate_key), values in grouped.items():
        selected = Counter(round(v, 4) for v in values).most_common(1)[0][0]
        rate_card.append(
            {
                "provider": "otto",
                "schaal": schaal or "NA",
                "tarief": tarief or "NA",
                "rate_key": rate_key,
                "rate_value": float(selected),
                "source_file": filename,
                "source_week": None,
            }
        )
    return ParsedRateWorkbook(person_rates=person_rates, rate_card=rate_card)


def parse_flex_wagegroup_rate_workbook(
    content: bytes, filename: str
) -> ParsedRateWorkbook:
    if load_workbook is None:
        raise ValueError("openpyxl is niet beschikbaar in deze omgeving.")
    wb = load_workbook(io.BytesIO(content), data_only=True)
    if "Blad1" in wb.sheetnames:
        ws = wb["Blad1"]
    elif "1 januari 2025" in wb.sheetnames:
        ws = wb["1 januari 2025"]
    else:
        raise ValueError(
            "Flex tarievenbestand moet sheet 'Blad1' of '1 januari 2025' bevatten."
        )

    header = [(_norm_text(c.value)) for c in ws[1]]
    by_name: dict[str, int] = {h.lower(): i for i, h in enumerate(header) if h}
    person_idx = by_name.get("loonnummer")
    first_name_idx = by_name.get("voornaam")
    last_name_idx = by_name.get("achternaam")
    loongroep_idx = by_name.get("loongroep")
    fase_contracten_idx = by_name.get("fase contracten")
    if loongroep_idx is None:
        loongroep_idx = 3  # D
    if fase_contracten_idx is None:
        fase_contracten_idx = 4  # E
    if person_idx is None:
        raise ValueError("Kolom 'Loonnummer' is verplicht.")
    if first_name_idx is None or last_name_idx is None:
        raise ValueError("Kolommen 'Voornaam' en 'Achternaam' zijn verplicht.")

    rate_columns = [
        ("100%", "100"),
        ("133%", "133"),
        ("135%", "135"),
        ("180%", "180"),
        ("200%", "200"),
        ("300%", "300"),
    ]
    # Fallback positions for Flex layout when % headers are missing.
    # J,L,N,P,R -> 100,133,135,200,300
    fallback_rate_indexes = {
        "100": 9,  # J
        "133": 11,  # L
        "135": 13,  # N
        "200": 15,  # P
        "300": 17,  # R
    }
    rate_indexes: list[tuple[int, str]] = []
    for col_name, rate_key in rate_columns:
        idx = by_name.get(col_name)
        if idx is None:
            idx = fallback_rate_indexes.get(rate_key)
        if idx is not None:
            rate_indexes.append((idx, rate_key))
    if not rate_indexes:
        raise ValueError(
            "Geen Flex tariefkolommen gevonden (verwacht 100%/133%/135%/180%/200%/300%)."
        )

    person_rates: list[dict[str, Any]] = []
    rate_card_candidates: dict[tuple[str, str, str], list[float]] = defaultdict(list)
    for row in ws.iter_rows(min_row=2, values_only=True):
        try:
            person_number = _to_person_number(
                row[person_idx] if person_idx < len(row) else None
            )
            if not person_number:
                continue
            first_name = _norm_text(
                row[first_name_idx] if first_name_idx < len(row) else None
            )
            last_name = _norm_text(
                row[last_name_idx] if last_name_idx < len(row) else None
            )
            name = _norm_text(f"{first_name} {last_name}")

            loongroep = _norm_text(
                row[loongroep_idx] if loongroep_idx < len(row) else None
            )
            schaal, parsed_tarief = _extract_schaal_tarief(loongroep)
            fase_contracten = _norm_text(
                row[fase_contracten_idx] if fase_contracten_idx < len(row) else None
            )
            fase = _extract_phase(fase_contracten)
            tarief = fase or parsed_tarief or "NA"
            schaal_value = schaal or "NA"

            for idx, rate_key in rate_indexes:
                rate_value = _to_float(row[idx] if idx < len(row) else None)
                if rate_value is None:
                    continue
                person_rates.append(
                    {
                        "provider": "flexspecialisten",
                        "person_number": person_number,
                        "name": name or person_number,
                        "normalized_name": normalize_person_name(name or person_number),
                        "schaal": schaal_value,
                        "tarief": tarief,
                        "rate_key": rate_key,
                        "rate_value": rate_value,
                        "source_file": filename,
                        "source_week": None,
                    }
                )
                rate_card_candidates[(schaal_value, tarief, rate_key)].append(
                    rate_value
                )
        except Exception:
            continue

    rate_card: list[dict[str, Any]] = []
    for (schaal, tarief, rate_key), values in rate_card_candidates.items():
        selected = Counter(round(v, 4) for v in values).most_common(1)[0][0]
        rate_card.append(
            {
                "provider": "flexspecialisten",
                "schaal": schaal,
                "tarief": tarief,
                "rate_key": rate_key,
                "rate_value": float(selected),
                "source_file": filename,
                "source_week": None,
            }
        )
    return ParsedRateWorkbook(person_rates=person_rates, rate_card=rate_card)


async def persist_parsed_wagegroup_rates(
    db: AsyncSession,
    *,
    provider: str,
    parsed: ParsedRateWorkbook,
) -> dict[str, int]:
    await db.execute(
        delete(PersonWagegroupRate).where(PersonWagegroupRate.provider == provider)
    )
    await db.execute(
        delete(WagegroupRateCard).where(WagegroupRateCard.provider == provider)
    )
    db.add_all(PersonWagegroupRate(**row) for row in parsed.person_rates)
    db.add_all(WagegroupRateCard(**row) for row in parsed.rate_card)
    await db.commit()
    return {
        "personRates": len(parsed.person_rates),
        "rateCardRows": len(parsed.rate_card),
    }


def write_rates_csvs(
    *, provider: str, parsed: ParsedRateWorkbook, output_dir: str
) -> dict[str, str]:
    os.makedirs(output_dir, exist_ok=True)
    person_path = os.path.join(output_dir, f"{provider}_wagegroup_person_rates.csv")
    card_path = os.path.join(output_dir, f"{provider}_wagegroup_rate_card.csv")
    with open(person_path, "w", newline="", encoding="utf-8-sig") as f:
        fields = [
            "provider",
            "person_number",
            "name",
            "normalized_name",
            "schaal",
            "tarief",
            "rate_key",
            "rate_value",
            "source_file",
            "source_week",
        ]
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(parsed.person_rates)
    with open(card_path, "w", newline="", encoding="utf-8-sig") as f:
        fields = [
            "provider",
            "schaal",
            "tarief",
            "rate_key",
            "rate_value",
            "source_file",
            "source_week",
        ]
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(parsed.rate_card)
    return {"personRatesCsv": person_path, "rateCardCsv": card_path}


def _invoice_line_rate(row: InvoiceLine) -> float | None:
    if row.totaal_uren and row.totaal_uren > 0:
        return row.subtotaal / row.totaal_uren
    if row.uurloon and row.uurloon > 0:
        return row.uurloon
    return None


def _write_rate_mismatches_csv(path: str, rows: list[dict[str, Any]]) -> None:
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        fields = [
            "sapId",
            "name",
            "invoiceCodeToeslag",
            "rateKey",
            "invoiceRate",
            "expectedRate",
            "difference",
            "status",
            "matchMethod",
        ]
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(rows)


def _build_histogram_rows(diffs: list[float]) -> list[dict[str, Any]]:
    bins = [
        (0, 0.1, "0.00-0.10"),
        (0.1, 0.25, "0.10-0.25"),
        (0.25, 0.5, "0.25-0.50"),
        (0.5, 1.0, "0.50-1.00"),
        (1.0, 2.0, "1.00-2.00"),
        (2.0, 5.0, "2.00-5.00"),
        (5.0, 999999.0, "5.00+"),
    ]
    rows = []
    for lo, hi, label in bins:
        count = sum(1 for d in diffs if lo <= d < hi)
        rows.append({"bucket": label, "count": count})
    return rows


def _write_histogram_csv(path: str, diffs: list[float]) -> None:
    rows = _build_histogram_rows(diffs)
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=["bucket", "count"])
        w.writeheader()
        w.writerows(rows)


async def analyze_otto_rate_mismatches(
    *,
    week: int,
    db: AsyncSession,
    tolerance_eur: float,
    output_dir: str,
) -> dict[str, Any]:
    person_rates_result = await db.execute(
        select(PersonWagegroupRate).where(PersonWagegroupRate.provider == "otto")
    )
    person_rates = list(person_rates_result.scalars().all())
    card_result = await db.execute(
        select(WagegroupRateCard).where(WagegroupRateCard.provider == "otto")
    )
    card_rows = list(card_result.scalars().all())
    known_result = await db.execute(
        select(PersonWagegroup).where(PersonWagegroup.provider == "otto")
    )
    known_rows = list(known_result.scalars().all())
    known_by_person = {r.person_number: r for r in known_rows}
    known_by_name = {normalize_person_name(r.name): r for r in known_rows}

    person_rate_lookup: dict[tuple[str, str], PersonWagegroupRate] = {}
    person_rate_name_lookup: dict[tuple[str, str], PersonWagegroupRate] = {}
    for row in person_rates:
        person_rate_lookup[(row.person_number, row.rate_key)] = row
        person_rate_name_lookup[(row.normalized_name, row.rate_key)] = row

    card_lookup: dict[tuple[str, str, str], WagegroupRateCard] = {}
    for row in card_rows:
        card_lookup[(row.schaal, row.tarief, row.rate_key)] = row

    invoice_result = await db.execute(
        select(InvoiceLine).where(
            InvoiceLine.week_number == week,
            InvoiceLine.agency == "otto",
        )
    )
    invoice_rows = list(invoice_result.scalars().all())

    mismatches: list[dict[str, Any]] = []
    differences: list[float] = []
    matched = 0
    missing_expected_rate = 0
    for row in invoice_rows:
        rate_key = _rate_key_from_code_toeslag(row.code_toeslag or "")
        invoice_rate = _invoice_line_rate(row)
        if invoice_rate is None:
            continue
        sap_id = _norm_text(row.sap_id)
        normalized_name = normalize_person_name(row.naam or "")
        known = known_by_person.get(sap_id) or known_by_name.get(normalized_name)
        person_rate = person_rate_lookup.get((sap_id, rate_key))
        match_method = "person_number"
        if not person_rate:
            person_rate = person_rate_name_lookup.get((normalized_name, rate_key))
            match_method = "name_fallback"
        expected_rate = person_rate.rate_value if person_rate else None
        if expected_rate is None and known:
            # fallback via card using known wagegroup as schaal+tarief hint
            schaal, tarief = _extract_schaal_tarief(known.wagegroup)
            if schaal and tarief:
                card = card_lookup.get((schaal, tarief, rate_key))
                if card:
                    expected_rate = card.rate_value
                    match_method = "rate_card"
        if expected_rate is None:
            missing_expected_rate += 1
            continue
        matched += 1
        diff = abs(invoice_rate - expected_rate)
        differences.append(diff)
        if diff <= tolerance_eur:
            continue
        mismatches.append(
            {
                "sapId": sap_id,
                "name": row.naam,
                "invoiceCodeToeslag": row.code_toeslag,
                "rateKey": rate_key,
                "invoiceRate": round(invoice_rate, 4),
                "expectedRate": round(expected_rate, 4),
                "difference": round(diff, 4),
                "status": "rate_mismatch",
                "matchMethod": match_method,
            }
        )

    os.makedirs(output_dir, exist_ok=True)
    mismatch_path = os.path.join(output_dir, f"{week} validation_wagerates_otto.csv")
    hist_path = os.path.join(output_dir, f"{week} rate_diff_histogram_otto.csv")
    _write_rate_mismatches_csv(mismatch_path, mismatches)
    _write_histogram_csv(hist_path, differences)

    return {
        "status": "ok",
        "week": week,
        "provider": "otto",
        "toleranceEur": tolerance_eur,
        "matchedRateRows": matched,
        "missingExpectedRate": missing_expected_rate,
        "rateMismatches": len(mismatches),
        "mismatches": mismatches,
        "outputFile": mismatch_path,
        "histogramFile": hist_path,
    }

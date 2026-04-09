from __future__ import annotations

import argparse
import asyncio
import csv
from collections import defaultdict
from pathlib import Path
from statistics import mean

try:
    import matplotlib.pyplot as plt  # pyright: ignore[reportMissingImports]
except ImportError:  # pragma: no cover - optional runtime dependency
    plt = None

from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from config import Settings
from models import InvoiceLine, PersonWagegroup, PersonWagegroupRate, WagegroupRateCard
from validation_wagegroups import normalize_person_name
from wagegroup_rates import _extract_schaal_tarief, _invoice_line_rate, _rate_key_from_code_toeslag


def _make_session_factory() -> async_sessionmaker[AsyncSession]:
    settings = Settings()
    database_url = settings.postgres_database_url
    if database_url.startswith("postgresql://"):
        database_url = database_url.replace("postgresql://", "postgresql+asyncpg://", 1)
    engine = create_async_engine(database_url, echo=False)
    return async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


def _buckets() -> list[tuple[float, float, str]]:
    return [
        (0.00, 0.02, "0.00-0.02"),
        (0.02, 0.05, "0.02-0.05"),
        (0.05, 0.10, "0.05-0.10"),
        (0.10, 0.15, "0.10-0.15"),
        (0.15, 0.20, "0.15-0.20"),
        (0.20, 0.30, "0.20-0.30"),
        (0.30, 0.40, "0.30-0.40"),
        (0.40, 0.50, "0.40-0.50"),
        (0.50, 0.75, "0.50-0.75"),
        (0.75, 1.00, "0.75-1.00"),
        (1.00, 1.25, "1.00-1.25"),
        (1.25, 1.50, "1.25-1.50"),
        (1.50, 2.00, "1.50-2.00"),
        (2.00, 3.00, "2.00-3.00"),
        (3.00, 5.00, "3.00-5.00"),
        (5.00, 10.00, "5.00-10.00"),
        (10.00, 999999.0, "10.00+"),
    ]


def _bucket_for_diff(diff: float) -> str:
    for lo, hi, label in _buckets():
        if lo <= diff < hi:
            return label
    return "5.00+"


async def diagnose_otto_week(week: int, tolerance: float, output_dir: Path) -> None:
    session_factory = _make_session_factory()
    async with session_factory() as db:
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

        resolved_week = week
        if week < 100000:
            week_candidates_result = await db.execute(
                select(InvoiceLine.week_number, func.count())
                .where(
                    InvoiceLine.agency == "otto",
                    InvoiceLine.week_number >= 100000,
                    (InvoiceLine.week_number % 100) == week,
                )
                .group_by(InvoiceLine.week_number)
                .order_by(InvoiceLine.week_number.desc())
            )
            week_candidates = [
                {"week_number": int(w or 0), "count": int(c or 0)}
                for w, c in week_candidates_result.all()
            ]
            if week_candidates:
                resolved_week = week_candidates[0]["week_number"]

        invoice_result = await db.execute(
            select(InvoiceLine).where(
                InvoiceLine.week_number == resolved_week,
                InvoiceLine.agency == "otto",
            )
        )
        invoice_rows = list(invoice_result.scalars().all())

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

    person_diffs: dict[str, list[float]] = defaultdict(list)
    person_names: dict[str, str] = {}
    all_diffs: list[float] = []

    matched_rows = 0
    missing_expected_rate = 0
    mismatches_over_tolerance = 0

    for row in invoice_rows:
        rate_key = _rate_key_from_code_toeslag(row.code_toeslag or "")
        invoice_rate = _invoice_line_rate(row)
        if invoice_rate is None:
            continue

        sap_id = str(row.sap_id or "").strip()
        normalized_name = normalize_person_name(row.naam or "")
        known = known_by_person.get(sap_id) or known_by_name.get(normalized_name)

        person_rate = person_rate_lookup.get((sap_id, rate_key))
        if not person_rate:
            person_rate = person_rate_name_lookup.get((normalized_name, rate_key))

        expected_rate = person_rate.rate_value if person_rate else None
        if expected_rate is None and known:
            schaal, tarief = _extract_schaal_tarief(known.wagegroup)
            if schaal and tarief:
                card = card_lookup.get((schaal, tarief, rate_key))
                if card:
                    expected_rate = card.rate_value

        if expected_rate is None:
            missing_expected_rate += 1
            continue

        matched_rows += 1
        diff = abs(invoice_rate - expected_rate)
        all_diffs.append(diff)
        if diff > tolerance:
            mismatches_over_tolerance += 1

        person_key = sap_id or normalized_name or "unknown"
        person_diffs[person_key].append(diff)
        person_names[person_key] = row.naam or sap_id or "unknown"

    print(f"\nOTTO rate diagnostic for requested week {week}")
    print(f"- Resolved DB week: {resolved_week}")
    print(f"- Invoice rows: {len(invoice_rows)}")
    print(f"- Matched expected rate: {matched_rows}")
    print(f"- Missing expected rate: {missing_expected_rate}")
    print(f"- Mismatches > {tolerance:.2f} EUR: {mismatches_over_tolerance}")

    ranked = sorted(
        person_diffs.items(),
        key=lambda kv: (mean(kv[1]), max(kv[1]), len(kv[1])),
        reverse=True,
    )

    print("\nTop 15 persons by mean absolute difference")
    print("person_key | person_name | rows | mean_diff | max_diff")
    for person_key, diffs in ranked[:15]:
        print(
            f"{person_key} | {person_names.get(person_key, '')} | "
            f"{len(diffs)} | {mean(diffs):.4f} | {max(diffs):.4f}"
        )

    output_dir.mkdir(parents=True, exist_ok=True)
    per_person_path = output_dir / f"{resolved_week}_otto_rate_diff_per_person.csv"
    histogram_path = output_dir / f"{resolved_week}_otto_rate_diff_histogram_per_person.csv"
    histogram_plot_path = output_dir / f"{resolved_week}_otto_rate_diff_histogram.png"

    with per_person_path.open("w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(
            f,
            fieldnames=["personKey", "personName", "rowCount", "meanDiff", "maxDiff"],
        )
        w.writeheader()
        for person_key, diffs in ranked:
            w.writerow(
                {
                    "personKey": person_key,
                    "personName": person_names.get(person_key, ""),
                    "rowCount": len(diffs),
                    "meanDiff": round(mean(diffs), 6),
                    "maxDiff": round(max(diffs), 6),
                }
            )

    with histogram_path.open("w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(
            f,
            fieldnames=["personKey", "personName", "bucket", "count"],
        )
        w.writeheader()
        for person_key, diffs in ranked:
            bucket_counts: dict[str, int] = defaultdict(int)
            for diff in diffs:
                bucket_counts[_bucket_for_diff(diff)] += 1
            for _, _, bucket in _buckets():
                w.writerow(
                    {
                        "personKey": person_key,
                        "personName": person_names.get(person_key, ""),
                        "bucket": bucket,
                        "count": bucket_counts.get(bucket, 0),
                    }
                )

    # Plot global histogram of absolute rate differences.
    if plt is None:
        print("Skipping histogram plot: matplotlib is not installed.")
    else:
        bucket_labels = [label for _, _, label in _buckets()]
        bucket_counts: list[int] = []
        for lo, hi, _ in _buckets():
            bucket_counts.append(sum(1 for d in all_diffs if lo <= d < hi))

        fig, ax = plt.subplots(figsize=(9, 4.5))
        ax.bar(bucket_labels, bucket_counts, color="#2F6DB3")
        ax.set_title(f"OTTO rate differences histogram ({resolved_week})")
        ax.set_xlabel("Absolute difference bucket (EUR)")
        ax.set_ylabel("Count")
        ax.tick_params(axis="x", rotation=30)
        ax.grid(axis="y", linestyle="--", alpha=0.35)
        fig.tight_layout()
        fig.savefig(histogram_plot_path, dpi=150)
        plt.close(fig)

    print(f"\nWrote: {per_person_path}")
    print(f"Wrote: {histogram_path}\n")
    if plt is not None:
        print(f"Wrote: {histogram_plot_path}\n")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run OTTO rate diagnostics for one week."
    )
    parser.add_argument(
        "--week",
        type=int,
        default=50,
        help="Week number to diagnose (default: 50).",
    )
    parser.add_argument(
        "--tolerance",
        type=float,
        default=1.0,
        help="Mismatch tolerance in EUR (default: 1.0).",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("output"),
        help="Output directory for CSV files (default: output).",
    )
    args = parser.parse_args()
    asyncio.run(
        diagnose_otto_week(
            week=args.week,
            tolerance=args.tolerance,
            output_dir=args.output_dir,
        )
    )


if __name__ == "__main__":
    main()

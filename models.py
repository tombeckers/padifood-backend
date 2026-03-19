from datetime import date, datetime
from typing import Optional

from sqlalchemy import UniqueConstraint
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class Kloklijst(Base):
    """
    Timesheet rows from either Otto Workforce or Flexspecialisten kloklijsten.
    The block structure (name only on first row per employee) is resolved before
    inserting — all rows carry the employee identifiers after forward-filling.
    """

    __tablename__ = "kloklijst"

    id: Mapped[int] = mapped_column(primary_key=True)
    week_number: Mapped[int]
    agency: Mapped[str]  # 'otto' or 'flexspecialisten'

    # Employee identifiers (forward-filled from first row of each block)
    loonnummers: Mapped[Optional[int]]
    personeelsnummer: Mapped[Optional[int]]
    naam: Mapped[Optional[str]]
    afdeling: Mapped[Optional[str]]

    # Day columns
    datum: Mapped[Optional[date]]
    start: Mapped[Optional[datetime]]
    eind: Mapped[Optional[datetime]]
    pauze_genomen_dag: Mapped[Optional[float]]
    pauze_afgetrokken_dag: Mapped[Optional[float]]
    pzcor_dag: Mapped[Optional[float]]
    norm_uren_dag: Mapped[Optional[float]]
    t133_dag: Mapped[Optional[float]]
    t135_dag: Mapped[Optional[float]]
    t200_dag: Mapped[Optional[float]]
    ow140_week: Mapped[Optional[float]]
    ow180_dag: Mapped[Optional[float]]
    ow200_dag: Mapped[Optional[float]]
    effectieve_uren_dag: Mapped[Optional[float]]


class InvoiceLine(Base):
    """
    Line items from the OTTO invoice (Padifood specificatie - Export Factuur).
    One row per surcharge type per employee per day.
    """

    __tablename__ = "invoice_lines"

    id: Mapped[int] = mapped_column(primary_key=True)
    week_number: Mapped[int]

    sap_id: Mapped[str]
    naam: Mapped[str]
    uurloon: Mapped[float]
    uurloon_zonder_atv: Mapped[float]
    functie_toeslag: Mapped[float]
    wekentelling: Mapped[int]
    fase_tarief: Mapped[str]
    datum: Mapped[date]
    code_toeslag: Mapped[str]
    totaal_uren: Mapped[float]
    subtotaal: Mapped[float]


class Tarievensheet(Base):
    """
    Rate sheet from the Padifood specificatie - Tarievensheet per persoon.
    Contains agreed billing rates per employee per day, plus pre-computed
    rates for each surcharge multiplier.
    """

    __tablename__ = "tarievensheet"

    id: Mapped[int] = mapped_column(primary_key=True)
    week_number: Mapped[int]

    sap_id: Mapped[str]
    naam: Mapped[str]
    uurloon: Mapped[float]
    uurloon_zonder_atv: Mapped[float]
    functie_toeslag: Mapped[float]
    wekentelling: Mapped[int]
    fase_tarief: Mapped[str]
    datum: Mapped[date]
    code_toeslag: Mapped[str]
    som_totaal_uren: Mapped[float]
    som_subtotaal: Mapped[float]

    # Billing rate and rate factors
    tarief: Mapped[Optional[float]]
    orf: Mapped[Optional[float]]
    marge: Mapped[Optional[float]]

    # Pre-computed rates per surcharge multiplier
    rate_norm: Mapped[Optional[float]]     # 1x
    rate_133: Mapped[Optional[float]]      # 1.33x
    rate_135: Mapped[Optional[float]]      # 1.35x
    rate_180_day: Mapped[Optional[float]]  # 1.8x day
    rate_200: Mapped[Optional[float]]      # 2x
    rate_300: Mapped[Optional[float]]      # 3x
    rate_140: Mapped[Optional[float]]      # 1.4x
    rate_180_ow: Mapped[Optional[float]]   # 1.8x overtime
    rate_200_ow: Mapped[Optional[float]]   # 2x overtime
    rate_300_ow: Mapped[Optional[float]]   # 3x overtime

    # Actual fase rates (may differ from invoice fase)
    fase_actual: Mapped[Optional[str]]
    orf_actual: Mapped[Optional[float]]


class OttoRateCard(Base):
    """
    OTTO rate card per employee (OTTO -Padifood tarievenoverzicht).
    Static reference data — not tied to a specific week.
    """

    __tablename__ = "otto_rate_card"

    id: Mapped[int] = mapped_column(primary_key=True)

    personeelsnummer: Mapped[Optional[int]]
    achternaam: Mapped[str]
    voornaam: Mapped[str]
    wekenteller: Mapped[Optional[int]]
    schaal: Mapped[Optional[str]]
    uurloon_incl_atv: Mapped[float]
    uurloon_excl_atv: Mapped[float]

    # Pre-computed billing rates
    rate_norm: Mapped[Optional[float]]     # 1x
    rate_133: Mapped[Optional[float]]      # 1.33x
    rate_135: Mapped[Optional[float]]      # 1.35x
    rate_180_day: Mapped[Optional[float]]  # 1.8x day
    rate_200: Mapped[Optional[float]]      # 2x
    rate_140: Mapped[Optional[float]]      # 1.4x OW
    rate_180_ow: Mapped[Optional[float]]   # 1.8x OW
    rate_200_ow: Mapped[Optional[float]]   # 2x OW


class OttoIdentifierMapping(Base):
    """
    Verified mapping between OTTO kloklijst Loonnummers and factuur SAP IDs.
    Used by validation as identifier-first matching key.
    """

    __tablename__ = "otto_identifier_mapping"
    __table_args__ = (
        UniqueConstraint("provider", "kloklijst_loonnummer", name="uq_otto_map_provider_loon"),
        UniqueConstraint("provider", "sap_id", name="uq_otto_map_provider_sap"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    provider: Mapped[str]
    kloklijst_loonnummer: Mapped[str]
    sap_id: Mapped[str]
    kloklijst_name: Mapped[str]
    factuur_name: Mapped[str]
    match_type: Mapped[str]
    verified: Mapped[bool]
    source_week: Mapped[Optional[int]]

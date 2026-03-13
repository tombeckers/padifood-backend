"""initial schema

Revision ID: 745e17ad7ef8
Revises:
Create Date: 2026-02-19

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "745e17ad7ef8"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "kloklijst",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("week_number", sa.Integer(), nullable=False),
        sa.Column("agency", sa.String(), nullable=False),
        sa.Column("loonnummers", sa.Integer(), nullable=True),
        sa.Column("personeelsnummer", sa.Integer(), nullable=True),
        sa.Column("naam", sa.String(), nullable=True),
        sa.Column("afdeling", sa.String(), nullable=True),
        sa.Column("datum", sa.Date(), nullable=True),
        sa.Column("start", sa.DateTime(), nullable=True),
        sa.Column("eind", sa.DateTime(), nullable=True),
        sa.Column("pauze_genomen_dag", sa.Float(), nullable=True),
        sa.Column("pauze_afgetrokken_dag", sa.Float(), nullable=True),
        sa.Column("pzcor_dag", sa.Float(), nullable=True),
        sa.Column("norm_uren_dag", sa.Float(), nullable=True),
        sa.Column("t133_dag", sa.Float(), nullable=True),
        sa.Column("t135_dag", sa.Float(), nullable=True),
        sa.Column("t200_dag", sa.Float(), nullable=True),
        sa.Column("ow140_week", sa.Float(), nullable=True),
        sa.Column("ow180_dag", sa.Float(), nullable=True),
        sa.Column("ow200_dag", sa.Float(), nullable=True),
        sa.Column("effectieve_uren_dag", sa.Float(), nullable=True),
    )
    op.create_index("ix_kloklijst_week_agency", "kloklijst", ["week_number", "agency"])

    op.create_table(
        "invoice_lines",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("week_number", sa.Integer(), nullable=False),
        sa.Column("sap_id", sa.String(), nullable=False),
        sa.Column("naam", sa.String(), nullable=False),
        sa.Column("uurloon", sa.Float(), nullable=False),
        sa.Column("uurloon_zonder_atv", sa.Float(), nullable=False),
        sa.Column("functie_toeslag", sa.Float(), nullable=False),
        sa.Column("wekentelling", sa.Integer(), nullable=False),
        sa.Column("fase_tarief", sa.String(), nullable=False),
        sa.Column("datum", sa.Date(), nullable=False),
        sa.Column("code_toeslag", sa.String(), nullable=False),
        sa.Column("totaal_uren", sa.Float(), nullable=False),
        sa.Column("subtotaal", sa.Float(), nullable=False),
    )
    op.create_index("ix_invoice_lines_week", "invoice_lines", ["week_number"])

    op.create_table(
        "tarievensheet",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("week_number", sa.Integer(), nullable=False),
        sa.Column("sap_id", sa.String(), nullable=False),
        sa.Column("naam", sa.String(), nullable=False),
        sa.Column("uurloon", sa.Float(), nullable=False),
        sa.Column("uurloon_zonder_atv", sa.Float(), nullable=False),
        sa.Column("functie_toeslag", sa.Float(), nullable=False),
        sa.Column("wekentelling", sa.Integer(), nullable=False),
        sa.Column("fase_tarief", sa.String(), nullable=False),
        sa.Column("datum", sa.Date(), nullable=False),
        sa.Column("code_toeslag", sa.String(), nullable=False),
        sa.Column("som_totaal_uren", sa.Float(), nullable=False),
        sa.Column("som_subtotaal", sa.Float(), nullable=False),
        sa.Column("tarief", sa.Float(), nullable=True),
        sa.Column("orf", sa.Float(), nullable=True),
        sa.Column("marge", sa.Float(), nullable=True),
        sa.Column("rate_norm", sa.Float(), nullable=True),
        sa.Column("rate_133", sa.Float(), nullable=True),
        sa.Column("rate_135", sa.Float(), nullable=True),
        sa.Column("rate_180_day", sa.Float(), nullable=True),
        sa.Column("rate_200", sa.Float(), nullable=True),
        sa.Column("rate_300", sa.Float(), nullable=True),
        sa.Column("rate_140", sa.Float(), nullable=True),
        sa.Column("rate_180_ow", sa.Float(), nullable=True),
        sa.Column("rate_200_ow", sa.Float(), nullable=True),
        sa.Column("rate_300_ow", sa.Float(), nullable=True),
        sa.Column("fase_actual", sa.String(), nullable=True),
        sa.Column("orf_actual", sa.Float(), nullable=True),
    )
    op.create_index("ix_tarievensheet_week", "tarievensheet", ["week_number"])

    op.create_table(
        "otto_rate_card",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("personeelsnummer", sa.Integer(), nullable=True),
        sa.Column("achternaam", sa.String(), nullable=False),
        sa.Column("voornaam", sa.String(), nullable=False),
        sa.Column("wekenteller", sa.Integer(), nullable=True),
        sa.Column("schaal", sa.String(), nullable=True),
        sa.Column("uurloon_incl_atv", sa.Float(), nullable=False),
        sa.Column("uurloon_excl_atv", sa.Float(), nullable=False),
        sa.Column("rate_norm", sa.Float(), nullable=True),
        sa.Column("rate_133", sa.Float(), nullable=True),
        sa.Column("rate_135", sa.Float(), nullable=True),
        sa.Column("rate_180_day", sa.Float(), nullable=True),
        sa.Column("rate_200", sa.Float(), nullable=True),
        sa.Column("rate_140", sa.Float(), nullable=True),
        sa.Column("rate_180_ow", sa.Float(), nullable=True),
        sa.Column("rate_200_ow", sa.Float(), nullable=True),
    )


def downgrade() -> None:
    op.drop_table("otto_rate_card")
    op.drop_index("ix_tarievensheet_week", "tarievensheet")
    op.drop_table("tarievensheet")
    op.drop_index("ix_invoice_lines_week", "invoice_lines")
    op.drop_table("invoice_lines")
    op.drop_index("ix_kloklijst_week_agency", "kloklijst")
    op.drop_table("kloklijst")

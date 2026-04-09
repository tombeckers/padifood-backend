"""add wagegroup rate tables

Revision ID: 9f3a1b2c4d5e
Revises: f2a8c1d4e9b7
Create Date: 2026-04-08

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "9f3a1b2c4d5e"
down_revision: Union[str, Sequence[str], None] = "f2a8c1d4e9b7"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "person_wagegroup_rates",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("provider", sa.String(), nullable=False),
        sa.Column("person_number", sa.String(), nullable=False),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column("normalized_name", sa.String(), nullable=False),
        sa.Column("schaal", sa.String(), nullable=True),
        sa.Column("tarief", sa.String(), nullable=True),
        sa.Column("rate_key", sa.String(), nullable=False),
        sa.Column("rate_value", sa.Float(), nullable=False),
        sa.Column("source_file", sa.String(), nullable=True),
        sa.Column("source_week", sa.Integer(), nullable=True),
    )
    op.create_unique_constraint(
        "uq_person_rate_provider_person_schaal_tarief_key",
        "person_wagegroup_rates",
        ["provider", "person_number", "schaal", "tarief", "rate_key"],
    )
    op.create_index(
        "ix_person_rate_provider_person",
        "person_wagegroup_rates",
        ["provider", "person_number"],
    )
    op.create_index(
        "ix_person_rate_provider_norm_name",
        "person_wagegroup_rates",
        ["provider", "normalized_name"],
    )

    op.create_table(
        "wagegroup_rate_card",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("provider", sa.String(), nullable=False),
        sa.Column("schaal", sa.String(), nullable=False),
        sa.Column("tarief", sa.String(), nullable=False),
        sa.Column("rate_key", sa.String(), nullable=False),
        sa.Column("rate_value", sa.Float(), nullable=False),
        sa.Column("source_file", sa.String(), nullable=True),
        sa.Column("source_week", sa.Integer(), nullable=True),
    )
    op.create_unique_constraint(
        "uq_rate_card_provider_schaal_tarief_key",
        "wagegroup_rate_card",
        ["provider", "schaal", "tarief", "rate_key"],
    )
    op.create_index(
        "ix_rate_card_provider_schaal_tarief",
        "wagegroup_rate_card",
        ["provider", "schaal", "tarief"],
    )


def downgrade() -> None:
    op.drop_index("ix_rate_card_provider_schaal_tarief", table_name="wagegroup_rate_card")
    op.drop_constraint("uq_rate_card_provider_schaal_tarief_key", "wagegroup_rate_card", type_="unique")
    op.drop_table("wagegroup_rate_card")

    op.drop_index("ix_person_rate_provider_norm_name", table_name="person_wagegroup_rates")
    op.drop_index("ix_person_rate_provider_person", table_name="person_wagegroup_rates")
    op.drop_constraint("uq_person_rate_provider_person_schaal_tarief_key", "person_wagegroup_rates", type_="unique")
    op.drop_table("person_wagegroup_rates")

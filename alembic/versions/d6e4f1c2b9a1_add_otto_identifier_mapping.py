"""add otto identifier mapping

Revision ID: d6e4f1c2b9a1
Revises: cb0fa2c7a69c
Create Date: 2026-03-19

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "d6e4f1c2b9a1"
down_revision: Union[str, Sequence[str], None] = "cb0fa2c7a69c"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "otto_identifier_mapping",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("provider", sa.String(), nullable=False),
        sa.Column("kloklijst_loonnummer", sa.String(), nullable=False),
        sa.Column("sap_id", sa.String(), nullable=False),
        sa.Column("kloklijst_name", sa.String(), nullable=False),
        sa.Column("factuur_name", sa.String(), nullable=False),
        sa.Column("match_type", sa.String(), nullable=False),
        sa.Column("verified", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("source_week", sa.Integer(), nullable=True),
    )
    op.create_index(
        "ix_otto_map_provider_verified",
        "otto_identifier_mapping",
        ["provider", "verified"],
    )
    op.create_unique_constraint(
        "uq_otto_map_provider_loon",
        "otto_identifier_mapping",
        ["provider", "kloklijst_loonnummer"],
    )
    op.create_unique_constraint(
        "uq_otto_map_provider_sap",
        "otto_identifier_mapping",
        ["provider", "sap_id"],
    )


def downgrade() -> None:
    op.drop_constraint("uq_otto_map_provider_sap", "otto_identifier_mapping", type_="unique")
    op.drop_constraint("uq_otto_map_provider_loon", "otto_identifier_mapping", type_="unique")
    op.drop_index("ix_otto_map_provider_verified", table_name="otto_identifier_mapping")
    op.drop_table("otto_identifier_mapping")

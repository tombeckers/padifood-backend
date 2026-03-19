"""add person wagegroups

Revision ID: f2a8c1d4e9b7
Revises: d6e4f1c2b9a1
Create Date: 2026-03-19

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "f2a8c1d4e9b7"
down_revision: Union[str, Sequence[str], None] = "d6e4f1c2b9a1"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "person_wagegroups",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("provider", sa.String(), nullable=False),
        sa.Column("person_number", sa.String(), nullable=False),
        sa.Column("kloklijst_loonnummer", sa.String(), nullable=True),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column("wagegroup", sa.String(), nullable=False),
        sa.Column("verified", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("source_week", sa.Integer(), nullable=True),
    )
    op.create_unique_constraint(
        "uq_wagegroup_provider_person",
        "person_wagegroups",
        ["provider", "person_number"],
    )
    op.create_index(
        "ix_wagegroup_provider_verified",
        "person_wagegroups",
        ["provider", "verified"],
    )


def downgrade() -> None:
    op.drop_index("ix_wagegroup_provider_verified", table_name="person_wagegroups")
    op.drop_constraint("uq_wagegroup_provider_person", "person_wagegroups", type_="unique")
    op.drop_table("person_wagegroups")

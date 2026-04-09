"""add agency to invoice_lines, make datum nullable

Revision ID: a1b2c3d4e5f6
Revises: cb0fa2c7a69c
Create Date: 2026-03-18 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect


revision: str = 'a1b2c3d4e5f6'
down_revision: Union[str, None] = 'cb0fa2c7a69c'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    bind = op.get_bind()
    columns = {c["name"] for c in inspect(bind).get_columns("invoice_lines")}

    # Add agency column only when missing, so reruns don't fail on partially-migrated DBs.
    if "agency" not in columns:
        op.add_column(
            'invoice_lines',
            sa.Column('agency', sa.String(), nullable=False, server_default='otto'),
        )

    # Make datum nullable — Flexspecialisten PDFs have weekly totals only (no per-day dates)
    if "datum" in columns:
        op.alter_column('invoice_lines', 'datum', existing_type=sa.Date(), nullable=True)


def downgrade() -> None:
    bind = op.get_bind()
    columns = {c["name"] for c in inspect(bind).get_columns("invoice_lines")}

    if "datum" in columns:
        op.alter_column('invoice_lines', 'datum', existing_type=sa.Date(), nullable=False)
    if "agency" in columns:
        op.drop_column('invoice_lines', 'agency')

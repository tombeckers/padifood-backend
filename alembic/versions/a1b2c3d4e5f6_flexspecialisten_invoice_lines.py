"""add agency to invoice_lines, make datum nullable

Revision ID: a1b2c3d4e5f6
Revises: cb0fa2c7a69c
Create Date: 2026-03-18 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = 'a1b2c3d4e5f6'
down_revision: Union[str, None] = 'cb0fa2c7a69c'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Add agency column — default 'otto' so all existing rows are correctly tagged
    op.add_column(
        'invoice_lines',
        sa.Column('agency', sa.String(), nullable=False, server_default='otto'),
    )
    # Make datum nullable — Flexspecialisten PDFs have weekly totals only (no per-day dates)
    op.alter_column('invoice_lines', 'datum', existing_type=sa.Date(), nullable=True)


def downgrade() -> None:
    op.alter_column('invoice_lines', 'datum', existing_type=sa.Date(), nullable=False)
    op.drop_column('invoice_lines', 'agency')

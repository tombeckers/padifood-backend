"""add tarief to invoice_lines

Revision ID: b3c7e2f1a4d8
Revises: 2c8f241c03c5
Create Date: 2026-04-16 00:00:00.000000

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "b3c7e2f1a4d8"
down_revision: Union[str, Sequence[str], None] = "2c8f241c03c5"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "invoice_lines",
        sa.Column("tarief", sa.Float(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("invoice_lines", "tarief")

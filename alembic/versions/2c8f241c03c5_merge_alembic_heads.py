"""merge alembic heads

Revision ID: 2c8f241c03c5
Revises: a1b2c3d4e5f6, 9f3a1b2c4d5e
Create Date: 2026-04-08 15:24:44.876260

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '2c8f241c03c5'
down_revision: Union[str, Sequence[str], None] = ('a1b2c3d4e5f6', '9f3a1b2c4d5e')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass

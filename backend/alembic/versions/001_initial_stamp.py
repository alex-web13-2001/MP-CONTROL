"""Initial stamp — baseline for existing schema.

All tables already exist in production and local via init.sql.
This migration serves as a starting point for Alembic tracking.

Revision ID: 001_initial_stamp
Revises: None
Create Date: 2026-02-20
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "001_initial_stamp"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Nothing to do — schema already exists via init.sql."""
    pass


def downgrade() -> None:
    """Nothing to do — we don't drop existing tables."""
    pass

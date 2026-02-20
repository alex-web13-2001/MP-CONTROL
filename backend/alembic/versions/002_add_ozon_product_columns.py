"""Add extended columns to dim_ozon_products.

18 new columns for product status, images hash, pricing index, etc.
Uses IF NOT EXISTS to be safe for re-runs on databases where
columns were already added manually.

Revision ID: 002_add_ozon_product_columns
Revises: 001_initial_stamp
Create Date: 2026-02-20
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "002_add_ozon_product_columns"
down_revision: Union[str, None] = "001_initial_stamp"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


# Columns to add with their SQL types and defaults
NEW_COLUMNS = [
    ("created_at_ozon", "TIMESTAMPTZ", None),
    ("updated_at_ozon", "TIMESTAMPTZ", None),
    ("vat", "FLOAT", "0"),
    ("type_id", "INTEGER", None),
    ("model_id", "BIGINT", None),
    ("model_count", "INTEGER", "0"),
    ("price_index_color", "VARCHAR(32)", "''"),
    ("price_index_value", "FLOAT", "0"),
    ("competitor_min_price", "FLOAT", "0"),
    ("is_kgt", "BOOLEAN", "FALSE"),
    ("status", "VARCHAR(64)", "''"),
    ("moderate_status", "VARCHAR(64)", "''"),
    ("status_name", "VARCHAR(128)", "''"),
    ("all_images_json", "TEXT", "'[]'"),
    ("images_hash", "VARCHAR(64)", "''"),
    ("primary_image_url", "TEXT", None),
    ("availability", "VARCHAR(64)", "''"),
    ("availability_source", "VARCHAR(64)", "''"),
]


def upgrade() -> None:
    conn = op.get_bind()
    for col_name, col_type, default in NEW_COLUMNS:
        default_clause = f" DEFAULT {default}" if default else ""
        conn.execute(sa.text(
            f"ALTER TABLE dim_ozon_products "
            f"ADD COLUMN IF NOT EXISTS {col_name} {col_type}{default_clause}"
        ))


def downgrade() -> None:
    conn = op.get_bind()
    for col_name, _, _ in reversed(NEW_COLUMNS):
        conn.execute(sa.text(
            f"ALTER TABLE dim_ozon_products DROP COLUMN IF EXISTS {col_name}"
        ))

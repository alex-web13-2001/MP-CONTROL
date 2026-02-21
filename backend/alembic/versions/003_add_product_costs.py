"""Add product_costs table for user-defined cost prices.

Enables margin calculation: price - cost - commission - logistics.

Revision ID: 003_add_product_costs
Revises: 002_add_ozon_product_columns
Create Date: 2026-02-22
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "003_add_product_costs"
down_revision: Union[str, None] = "002_add_ozon_product_columns"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    conn = op.get_bind()

    # Create table (IF NOT EXISTS for safety on re-runs)
    conn.execute(sa.text("""
        CREATE TABLE IF NOT EXISTS product_costs (
            id SERIAL PRIMARY KEY,
            shop_id INTEGER NOT NULL REFERENCES shops(id) ON DELETE CASCADE,
            offer_id VARCHAR(100) NOT NULL,
            cost_price DECIMAL(12, 2) NOT NULL DEFAULT 0,
            packaging_cost DECIMAL(12, 2) DEFAULT 0,
            notes TEXT,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE(shop_id, offer_id)
        )
    """))

    conn.execute(sa.text(
        "CREATE INDEX IF NOT EXISTS idx_product_costs_shop ON product_costs(shop_id)"
    ))
    conn.execute(sa.text(
        "CREATE INDEX IF NOT EXISTS idx_product_costs_offer ON product_costs(offer_id)"
    ))

    # Trigger for auto-updating updated_at
    conn.execute(sa.text("""
        DROP TRIGGER IF EXISTS update_product_costs_updated_at ON product_costs
    """))
    conn.execute(sa.text("""
        CREATE TRIGGER update_product_costs_updated_at
        BEFORE UPDATE ON product_costs
        FOR EACH ROW EXECUTE FUNCTION update_updated_at_column()
    """))


def downgrade() -> None:
    op.drop_table("product_costs")

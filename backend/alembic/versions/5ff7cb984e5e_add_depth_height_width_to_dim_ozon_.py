"""add_depth_height_width_to_dim_ozon_products

Revision ID: 5ff7cb984e5e
Revises: 003_add_product_costs
Create Date: 2026-03-11 11:30:16.729501
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = '5ff7cb984e5e'
down_revision: Union[str, None] = '003_add_product_costs'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('dim_ozon_products', sa.Column('depth', sa.Float(), nullable=True))
    op.add_column('dim_ozon_products', sa.Column('height', sa.Float(), nullable=True))
    op.add_column('dim_ozon_products', sa.Column('width', sa.Float(), nullable=True))


def downgrade() -> None:
    op.drop_column('dim_ozon_products', 'width')
    op.drop_column('dim_ozon_products', 'height')
    op.drop_column('dim_ozon_products', 'depth')

"""add_imt_id_to_dim_products

Revision ID: 2e105ad65fd8
Revises: b81f3ce45f30
Create Date: 2026-03-23 20:16:19.072572
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = '2e105ad65fd8'
down_revision: Union[str, None] = 'b81f3ce45f30'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('dim_products', sa.Column('imt_id', sa.BigInteger(), nullable=True))
    op.create_index(op.f('ix_dim_products_imt_id'), 'dim_products', ['imt_id'], unique=False)


def downgrade() -> None:
    op.drop_index(op.f('ix_dim_products_imt_id'), table_name='dim_products')
    op.drop_column('dim_products', 'imt_id')

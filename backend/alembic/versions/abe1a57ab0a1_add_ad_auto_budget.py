"""add_ad_auto_budget

Revision ID: abe1a57ab0a1
Revises: c3f8a9d72e01
Create Date: 2026-04-05 00:05:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'abe1a57ab0a1'
down_revision: Union[str, None] = 'c3f8a9d72e01'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table('ad_auto_budget',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('shop_id', sa.Integer(), nullable=False),
        sa.Column('advert_id', sa.BigInteger(), nullable=False),
        sa.Column('enabled', sa.Boolean(), nullable=False, server_default='false'),
        sa.Column('threshold', sa.Integer(), nullable=False, server_default='500'),
        sa.Column('amount', sa.Integer(), nullable=False, server_default='1000'),
        sa.Column('budget_type', sa.Integer(), nullable=False, server_default='1'),
        sa.Column('max_per_day', sa.Integer(), nullable=False, server_default='5'),
        sa.Column('deposits_today', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('last_deposit_at', sa.DateTime(), nullable=True),
        sa.Column('last_reset_date', sa.Date(), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.text('now()')),
        sa.Column('updated_at', sa.DateTime(), nullable=False, server_default=sa.text('now()')),
        sa.ForeignKeyConstraint(['shop_id'], ['shops.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_index(op.f('ix_ad_auto_budget_shop_id'), 'ad_auto_budget', ['shop_id'], unique=False)
    op.create_index(op.f('ix_ad_auto_budget_advert_id'), 'ad_auto_budget', ['advert_id'], unique=True)


def downgrade() -> None:
    op.drop_index(op.f('ix_ad_auto_budget_advert_id'), table_name='ad_auto_budget')
    op.drop_index(op.f('ix_ad_auto_budget_shop_id'), table_name='ad_auto_budget')
    op.drop_table('ad_auto_budget')

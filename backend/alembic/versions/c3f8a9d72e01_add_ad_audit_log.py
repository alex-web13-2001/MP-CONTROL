"""add ad_audit_log table

Revision ID: c3f8a9d72e01
Revises: 2e105ad65fd8
Create Date: 2026-03-30 20:36:00.000000
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = 'c3f8a9d72e01'
down_revision: Union[str, None] = '2e105ad65fd8'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table('ad_audit_log',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('created_at', sa.DateTime(), nullable=False,
                  server_default=sa.text('now()')),
        sa.Column('user_id', postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column('shop_id', sa.Integer(), nullable=False),
        sa.Column('action', sa.String(length=50), nullable=False),
        sa.Column('advert_id', sa.BigInteger(), nullable=True),
        sa.Column('details', sa.JSON(), nullable=True),
        sa.Column('success', sa.String(length=10), nullable=False,
                  server_default='true'),
        sa.Column('error_message', sa.String(length=500), nullable=True),
        sa.ForeignKeyConstraint(['user_id'], ['users.id'], ondelete='SET NULL'),
        sa.ForeignKeyConstraint(['shop_id'], ['shops.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_index('ix_ad_audit_log_user_id', 'ad_audit_log', ['user_id'])
    op.create_index('ix_ad_audit_log_shop_id', 'ad_audit_log', ['shop_id'])
    op.create_index('ix_ad_audit_log_created_at', 'ad_audit_log',
                     ['created_at'], postgresql_using='btree')


def downgrade() -> None:
    op.drop_index('ix_ad_audit_log_created_at', table_name='ad_audit_log')
    op.drop_index('ix_ad_audit_log_shop_id', table_name='ad_audit_log')
    op.drop_index('ix_ad_audit_log_user_id', table_name='ad_audit_log')
    op.drop_table('ad_audit_log')

"""
AdAuditLog model — logs all user-initiated advertising management actions.

Every WRITE operation (start, pause, stop, bid_change) is recorded here
for accountability and rollback analysis.
"""
from datetime import datetime

from sqlalchemy import (
    Column,
    Integer,
    BigInteger,
    String,
    DateTime,
    ForeignKey,
    JSON,
)
from sqlalchemy.dialects.postgresql import UUID

from app.core.database import Base


class AdAuditLog(Base):
    """
    Audit log for advertising management actions.

    Actions:
    - 'campaign_start': Campaign was started
    - 'campaign_pause': Campaign was paused
    - 'campaign_stop': Campaign was stopped (irreversible)
    - 'bid_change': Bid(s) changed for nm_id(s)
    - 'batch_status': Batch start/pause operation
    - 'batch_bid_change': Batch bid modification
    """

    __tablename__ = "ad_audit_log"

    id = Column(Integer, primary_key=True, autoincrement=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    user_id = Column(
        UUID(as_uuid=True),
        ForeignKey("users.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    shop_id = Column(
        Integer,
        ForeignKey("shops.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    action = Column(String(50), nullable=False)  # 'campaign_start', 'bid_change', etc.
    advert_id = Column(BigInteger, nullable=True)  # NULL for batch operations

    # Full details as JSONB (old/new values, nm_ids, placement, etc.)
    details = Column(JSON, nullable=True)

    # Whether WB API returned success
    success = Column(String(10), nullable=False, default="true")  # 'true'/'false'
    error_message = Column(String(500), nullable=True)

    def __repr__(self):
        return (
            f"<AdAuditLog action={self.action} advert={self.advert_id} "
            f"at {self.created_at}>"
        )

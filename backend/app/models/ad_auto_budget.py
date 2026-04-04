"""
AdAutoBudget model — stores auto-replenishment settings per campaign.

Each WB advertising campaign can have auto-budget rules:
- threshold: replenish when budget drops below this value (₽)
- amount: how much to deposit each time (₽)
- max_per_day: safety limit on daily deposits
- budget_type: fund source (0=Account, 1=Balance, 3=Bonuses)

The Celery task `sync_wb_budgets` checks these rules every 15 minutes
and automatically calls deposit_budget when conditions are met.
"""
from datetime import datetime, date

from sqlalchemy import (
    Column,
    Integer,
    BigInteger,
    Boolean,
    DateTime,
    Date,
    ForeignKey,
)

from app.core.database import Base


class AdAutoBudget(Base):
    """
    Auto-replenishment settings for WB advertising campaigns.

    One record per advert_id. Created when user enables auto-budget
    for the first time, updated on subsequent saves.
    """

    __tablename__ = "ad_auto_budget"

    id = Column(Integer, primary_key=True, autoincrement=True)

    shop_id = Column(
        Integer,
        ForeignKey("shops.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    advert_id = Column(BigInteger, nullable=False, unique=True, index=True)

    # — Settings —
    enabled = Column(Boolean, default=False, nullable=False)
    threshold = Column(Integer, default=500, nullable=False)     # ₽ — replenish when below
    amount = Column(Integer, default=1000, nullable=False)       # ₽ — deposit amount
    budget_type = Column(Integer, default=1, nullable=False)     # 0=Account, 1=Balance, 3=Bonuses
    max_per_day = Column(Integer, default=5, nullable=False)     # Max deposits per day

    # — Execution state —
    deposits_today = Column(Integer, default=0, nullable=False)
    last_deposit_at = Column(DateTime, nullable=True)
    last_reset_date = Column(Date, nullable=True)

    # — Timestamps —
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False,
    )

    def __repr__(self):
        return (
            f"<AdAutoBudget advert={self.advert_id} enabled={self.enabled} "
            f"threshold={self.threshold}₽ amount={self.amount}₽>"
        )

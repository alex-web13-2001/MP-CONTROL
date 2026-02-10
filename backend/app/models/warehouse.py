"""
DimWarehouse model — Warehouses dictionary for Commercial Monitoring.

Stores WB warehouse reference data (name, address, city).
Updated by:
  - wb_warehouses_service (daily): full refresh from /api/v1/offices
  - wb_stocks_service (on-the-fly): auto-creates unverified entries for unknown warehouses
"""
from datetime import datetime
from typing import Optional

from sqlalchemy import Boolean, Column, DateTime, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from app.core.database import Base


class DimWarehouse(Base):
    """WB warehouse dictionary."""

    __tablename__ = "dim_warehouses"

    warehouse_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=False)
    name: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    address: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    city: Mapped[Optional[str]] = mapped_column(String(150), nullable=True)
    is_verified: Mapped[bool] = mapped_column(Boolean, default=False)

    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    def __repr__(self):
        return f"<DimWarehouse id={self.warehouse_id} name={self.name}>"

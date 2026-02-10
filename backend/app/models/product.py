"""
DimProduct model — Products dictionary for Commercial Monitoring.

Stores product reference data: pricing, dimensions, images.
Updated by:
  - wb_prices_service (every 30 min): current_price, current_discount
  - wb_content_service (daily): name, main_image_url, dimensions, category
"""
from datetime import datetime
from decimal import Decimal
from typing import Optional

from sqlalchemy import (
    BigInteger, Column, DateTime, Integer, Numeric,
    String, Text, UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column

from app.core.database import Base


class DimProduct(Base):
    """Product dictionary for commercial monitoring."""

    __tablename__ = "dim_products"
    __table_args__ = (
        UniqueConstraint("shop_id", "nm_id", name="uq_dim_products_shop_nm"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    shop_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    nm_id: Mapped[int] = mapped_column(BigInteger, nullable=False, index=True)
    vendor_code: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    name: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)
    main_image_url: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # Dimensions (cm) — for logistics cost calculation
    length: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 2), nullable=True)
    width: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 2), nullable=True)
    height: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 2), nullable=True)

    # Current pricing (updated every 30 min by prices service)
    current_price: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 2), nullable=True)
    current_discount: Mapped[int] = mapped_column(Integer, default=0)

    # Classification
    category: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    def __repr__(self):
        return f"<DimProduct nm_id={self.nm_id} vendor={self.vendor_code} price={self.current_price}>"

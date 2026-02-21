"""
ProductCost model — user-defined cost prices for margin calculation.

Table: product_costs
Works for both Ozon and WB (unified by offer_id per shop).
"""
from datetime import datetime
from decimal import Decimal
from typing import Optional

from sqlalchemy import (
    DateTime, Integer, Numeric, String, Text, text,
)
from sqlalchemy.orm import Mapped, mapped_column

from app.core.database import Base


class ProductCost(Base):
    """User-defined product cost price."""

    __tablename__ = "product_costs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    shop_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    offer_id: Mapped[str] = mapped_column(String(100), nullable=False, index=True)

    cost_price: Mapped[Decimal] = mapped_column(
        Numeric(12, 2), nullable=False, server_default=text("0")
    )
    packaging_cost: Mapped[Decimal] = mapped_column(
        Numeric(12, 2), nullable=True, server_default=text("0")
    )
    notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=text("NOW()")
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=text("NOW()")
    )

    def __repr__(self):
        return f"<ProductCost shop={self.shop_id} offer={self.offer_id} cost={self.cost_price}>"

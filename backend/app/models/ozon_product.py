"""
DimOzonProduct model — Ozon product catalog (PostgreSQL).

Matches dim_ozon_products table from init.sql + extended columns.
Updated by sync_ozon_products task via psycopg2 upsert.
"""
from datetime import datetime
from decimal import Decimal
from typing import Optional

from sqlalchemy import (
    BigInteger, Boolean, DateTime, Float, Integer,
    Numeric, String, Text, UniqueConstraint, text,
)
from sqlalchemy.orm import Mapped, mapped_column

from app.core.database import Base


class DimOzonProduct(Base):
    """Ozon product catalog entry."""

    __tablename__ = "dim_ozon_products"
    __table_args__ = (
        UniqueConstraint("shop_id", "product_id", name="dim_ozon_products_shop_id_product_id_key"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    shop_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    product_id: Mapped[int] = mapped_column(BigInteger, nullable=False, index=True)
    offer_id: Mapped[str] = mapped_column(String(100), nullable=False, index=True)
    sku: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)
    name: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)
    main_image_url: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    barcode: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    category_id: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)

    # Pricing
    price: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 2), nullable=True)
    old_price: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 2), nullable=True)
    min_price: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 2), nullable=True)
    marketing_price: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 2), nullable=True)

    # Dimensions
    volume_weight: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 2), nullable=True)

    # Stocks
    stocks_fbo: Mapped[int] = mapped_column(Integer, default=0, server_default=text("0"))
    stocks_fbs: Mapped[int] = mapped_column(Integer, default=0, server_default=text("0"))

    # Flags
    is_archived: Mapped[bool] = mapped_column(Boolean, default=False, server_default=text("false"))
    has_fbo_stocks: Mapped[bool] = mapped_column(Boolean, default=False, server_default=text("false"))
    has_fbs_stocks: Mapped[bool] = mapped_column(Boolean, default=False, server_default=text("false"))

    # Extended fields (added via migration 002)
    created_at_ozon: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    updated_at_ozon: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    vat: Mapped[float] = mapped_column(Float, default=0, server_default=text("0"))
    type_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    model_id: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)
    model_count: Mapped[int] = mapped_column(Integer, default=0, server_default=text("0"))
    price_index_color: Mapped[str] = mapped_column(String(32), default="", server_default=text("''"))
    price_index_value: Mapped[float] = mapped_column(Float, default=0, server_default=text("0"))
    competitor_min_price: Mapped[float] = mapped_column(Float, default=0, server_default=text("0"))
    is_kgt: Mapped[bool] = mapped_column(Boolean, default=False, server_default=text("false"))
    status: Mapped[str] = mapped_column(String(64), default="", server_default=text("''"))
    moderate_status: Mapped[str] = mapped_column(String(64), default="", server_default=text("''"))
    status_name: Mapped[str] = mapped_column(String(128), default="", server_default=text("''"))
    all_images_json: Mapped[str] = mapped_column(Text, default="[]", server_default=text("'[]'"))
    images_hash: Mapped[str] = mapped_column(String(64), default="", server_default=text("''"))
    primary_image_url: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    availability: Mapped[str] = mapped_column(String(64), default="", server_default=text("''"))
    availability_source: Mapped[str] = mapped_column(String(64), default="", server_default=text("''"))

    # Timestamps
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=text("NOW()")
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=text("NOW()")
    )

    def __repr__(self):
        return f"<DimOzonProduct shop={self.shop_id} offer={self.offer_id} sku={self.sku}>"


class DimOzonProductContent(Base):
    """Ozon product content hashes for change detection."""

    __tablename__ = "dim_ozon_product_content"
    __table_args__ = (
        UniqueConstraint("shop_id", "product_id", name="dim_ozon_product_content_shop_id_product_id_key"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    shop_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    product_id: Mapped[int] = mapped_column(BigInteger, nullable=False, index=True)

    title_hash: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    description_hash: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    main_image_url: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    images_hash: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    images_count: Mapped[int] = mapped_column(Integer, default=0, server_default=text("0"))

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=text("NOW()")
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=text("NOW()")
    )

    def __repr__(self):
        return f"<DimOzonProductContent shop={self.shop_id} product={self.product_id}>"

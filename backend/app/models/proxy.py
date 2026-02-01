"""SQLAlchemy model for Proxy."""

from datetime import datetime
from decimal import Decimal
from typing import Optional

from sqlalchemy import (
    Boolean,
    DateTime,
    Integer,
    Numeric,
    String,
    Text,
    LargeBinary,
)
from sqlalchemy.orm import Mapped, mapped_column

from app.core.database import Base


class Proxy(Base):
    """Proxy server for request rotation."""
    
    __tablename__ = "proxies"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    
    # Connection info
    host: Mapped[str] = mapped_column(String(255), nullable=False)
    port: Mapped[int] = mapped_column(Integer, nullable=False)
    username: Mapped[Optional[str]] = mapped_column(String(255))
    password_encrypted: Mapped[Optional[bytes]] = mapped_column(LargeBinary)
    
    # Proxy type
    protocol: Mapped[str] = mapped_column(String(20), default="http")
    proxy_type: Mapped[str] = mapped_column(String(50), default="datacenter")
    
    # Location
    country: Mapped[str] = mapped_column(String(10), default="RU")
    region: Mapped[Optional[str]] = mapped_column(String(100))
    
    # Health tracking
    status: Mapped[str] = mapped_column(String(20), default="active")
    success_count: Mapped[int] = mapped_column(Integer, default=0)
    failure_count: Mapped[int] = mapped_column(Integer, default=0)
    last_success_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    last_failure_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    last_checked_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    
    # Success rate
    success_rate: Mapped[Decimal] = mapped_column(Numeric(5, 4), default=1.0)
    
    # Rate limiting
    requests_per_minute: Mapped[int] = mapped_column(Integer, default=60)
    current_minute_requests: Mapped[int] = mapped_column(Integer, default=0)
    current_minute_start: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    
    # Metadata
    provider: Mapped[Optional[str]] = mapped_column(String(100))
    notes: Mapped[Optional[str]] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=datetime.utcnow
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow
    )

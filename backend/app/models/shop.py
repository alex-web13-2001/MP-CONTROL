"""
Shop model — Marketplace store connection.

Matches existing init.sql schema (user_id UUID FK, api_key + api_key_encrypted).
"""
import uuid
from datetime import datetime
from typing import Optional

from sqlalchemy import (
    Boolean, DateTime, ForeignKey, Integer, LargeBinary,
    String, Text, text,
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.core.database import Base


class Shop(Base):
    """Marketplace shop connected by a user."""

    __tablename__ = "shops"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=True,
        index=True,
    )
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    marketplace: Mapped[str] = mapped_column(String(50), nullable=False)

    # API credentials
    api_key: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)
    api_key_encrypted: Mapped[Optional[bytes]] = mapped_column(LargeBinary, nullable=True)
    client_id: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)  # Ozon Client-Id

    # Ozon Performance API credentials (separate OAuth2)
    perf_client_id: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    perf_client_secret_encrypted: Mapped[Optional[bytes]] = mapped_column(LargeBinary, nullable=True)

    is_active: Mapped[bool] = mapped_column(Boolean, default=True, server_default=text("true"))

    # Circuit Breaker status
    status: Mapped[Optional[str]] = mapped_column(String(50), default="active", server_default=text("'active'"))
    status_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    last_sync_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=text("NOW()")
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=text("NOW()")
    )

    # Relationships
    owner = relationship("User", back_populates="shops")

    def __repr__(self):
        return f"<Shop id={self.id} name={self.name} marketplace={self.marketplace}>"

"""Pydantic schemas for authentication and shop management."""

from typing import Optional

from pydantic import BaseModel, EmailStr, Field


# ── Auth Request/Response ─────────────────────────────────────────

class RegisterRequest(BaseModel):
    """Registration request."""
    email: EmailStr
    password: str = Field(..., min_length=6, max_length=128)
    name: str = Field(..., min_length=1, max_length=255)


class LoginRequest(BaseModel):
    """Login request."""
    email: EmailStr
    password: str


class RefreshRequest(BaseModel):
    """Token refresh request."""
    refresh_token: str


# ── User Response ─────────────────────────────────────────────────

class ShopResponse(BaseModel):
    """Shop in API responses."""
    id: int
    name: str
    marketplace: str
    is_active: bool

    model_config = {"from_attributes": True}


class UserResponse(BaseModel):
    """User profile in API responses."""
    id: str  # UUID as string
    email: str
    name: str  # mapped from full_name
    is_active: bool
    shops: list[ShopResponse] = []

    model_config = {"from_attributes": True}


class TokenResponse(BaseModel):
    """JWT token pair response."""
    access_token: str
    refresh_token: str
    token_type: str = "bearer"
    user: UserResponse


# ── Shop Management ───────────────────────────────────────────────

class ShopCreate(BaseModel):
    """Create a new shop connection."""
    name: str = Field(..., min_length=1, max_length=255)
    marketplace: str = Field(..., pattern=r"^(wildberries|ozon)$")
    api_key: str = Field(..., min_length=1)
    client_id: Optional[str] = None  # Required for Ozon

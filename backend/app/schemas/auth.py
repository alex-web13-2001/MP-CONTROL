"""Pydantic schemas for authentication and shop management."""

from typing import Literal, Optional

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
    status: Optional[str] = "active"

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
    marketplace: Literal["wildberries", "ozon"]
    api_key: str = Field(..., min_length=1)
    client_id: Optional[str] = None           # Ozon Seller Client-Id
    perf_client_id: Optional[str] = None      # Ozon Performance Client-Id
    perf_client_secret: Optional[str] = None  # Ozon Performance Client-Secret


class ShopUpdateKeys(BaseModel):
    """Update API keys for an existing shop."""
    api_key: str = Field(..., min_length=1)
    client_id: Optional[str] = None           # Ozon Seller Client-Id
    perf_client_id: Optional[str] = None      # Ozon Performance Client-Id
    perf_client_secret: Optional[str] = None  # Ozon Performance Client-Secret


# ── Key Validation ────────────────────────────────────────────────

class ValidateKeyRequest(BaseModel):
    """Validate marketplace API key with a test request."""
    marketplace: Literal["wildberries", "ozon"]
    api_key: str = Field(..., min_length=1)
    client_id: Optional[str] = None
    perf_client_id: Optional[str] = None
    perf_client_secret: Optional[str] = None


class ValidateKeyResponse(BaseModel):
    """Result of API key validation."""
    valid: bool
    seller_valid: Optional[bool] = None    # Ozon seller check
    perf_valid: Optional[bool] = None      # Ozon performance check
    message: str
    shop_name: Optional[str] = None        # Auto-detected shop name
    warnings: Optional[list[str]] = None   # Permission warnings (WB /ping checks)



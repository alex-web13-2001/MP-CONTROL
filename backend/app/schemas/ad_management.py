"""Pydantic schemas for advertising management API."""

from typing import List, Optional, Literal

from pydantic import BaseModel, Field


# ── Campaign List Response ────────────────────────────────────────

class NmSettingResponse(BaseModel):
    """Per-nm_id bid settings in a campaign."""
    nm_id: int
    bid_search: int = Field(0, description="Search bid in kopecks")
    bid_recommendations: int = Field(0, description="Recommendations bid in kopecks")
    subject_name: str = ""
    product_name: str = ""
    vendor_code: str = ""


class CampaignWithBidsResponse(BaseModel):
    """Campaign with full bid details for management UI."""
    advert_id: int
    name: str
    type: int = Field(9, description="Campaign type (9 = unified)")
    status: int
    status_label: str
    payment_type: str = ""
    bid_type: str = ""
    search_enabled: bool = False
    recommendations_enabled: bool = False
    change_time: Optional[str] = None
    nm_settings: List[NmSettingResponse] = []


class CampaignsListResponse(BaseModel):
    """Response for campaigns list endpoint."""
    campaigns: List[CampaignWithBidsResponse]
    total: int
    balance: Optional[dict] = None


# ── Status Control ────────────────────────────────────────────────

class StatusChangeRequest(BaseModel):
    """Request to change campaign status."""
    shop_id: int
    advert_id: int


class StatusChangeResponse(BaseModel):
    """Response for campaign status change."""
    success: bool
    message: str
    status_code: int = 200


# ── Bid Management ────────────────────────────────────────────────

class BidItem(BaseModel):
    """Single bid value for a nm_id."""
    nm_id: int
    bid: int = Field(..., gt=0, description="Bid in kopecks (e.g. 15000 = 150₽)")


class ChangeBidsRequest(BaseModel):
    """Request to change bids in a campaign."""
    shop_id: int
    advert_id: int
    placement: Literal["search", "recommendations", "combined"]
    bids: List[BidItem] = Field(..., min_length=1, max_length=100)
    bid_type: str = Field("manual", description="Campaign bid_type: 'manual' or 'unified'")


class ChangeBidsResponse(BaseModel):
    """Response for bid change operation."""
    success: bool
    message: str
    status_code: int = 200
    bids_applied: int = 0


# ── Batch Operations ─────────────────────────────────────────────

class BatchStatusRequest(BaseModel):
    """Batch status change for multiple campaigns."""
    shop_id: int
    advert_ids: List[int] = Field(..., min_length=1, max_length=50)
    action: Literal["start", "pause"]


class BatchStatusResult(BaseModel):
    """Result for one campaign in a batch."""
    advert_id: int
    success: bool
    message: str


class BatchStatusResponse(BaseModel):
    """Response for batch status operation."""
    results: List[BatchStatusResult]
    total: int
    success_count: int
    failed_count: int


# ── Batch Bid Change ─────────────────────────────────────────────

class BatchBidChangeRequest(BaseModel):
    """Batch bid change for multiple campaigns.
    
    mode:
    - 'absolute': set bids to exact value (bid_value_kopecks)
    - 'percent_up': increase all bids by percent
    - 'percent_down': decrease all bids by percent
    """
    shop_id: int
    advert_ids: List[int] = Field(..., min_length=1, max_length=50)
    placement: Literal["search", "recommendations"]
    mode: Literal["absolute", "percent_up", "percent_down"]
    value: int = Field(
        ..., gt=0,
        description="Kopecks for absolute, percent for percent_up/down",
    )


# ── Balance ───────────────────────────────────────────────────────

class BalanceResponse(BaseModel):
    """Advertising balance."""
    success: bool
    balance: Optional[float] = None
    net: Optional[float] = None
    bonus: Optional[float] = None
    message: str = ""


# ── Audit Log ─────────────────────────────────────────────────────

class AuditLogEntry(BaseModel):
    """Single audit log entry."""
    id: int
    action: str
    advert_id: Optional[int] = None
    details: Optional[dict] = None
    created_at: str
    user_name: Optional[str] = None


class AuditLogResponse(BaseModel):
    """Response for audit log endpoint."""
    entries: List[AuditLogEntry]
    total: int

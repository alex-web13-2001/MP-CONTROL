"""
Finance Reports API endpoints.

Endpoints for downloading and managing WB/Ozon finance reports.
"""

from datetime import date
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from celery.result import AsyncResult

from celery_app.tasks.tasks import sync_wb_finance_history


# ... (skipping unchanged parts)




router = APIRouter(prefix="/finance-reports", tags=["Finance Reports"])


# ===================
# Schemas
# ===================

class SyncReportsRequest(BaseModel):
    """Request to sync finance reports for N months."""
    shop_id: int = Field(..., description="Shop ID in our system")
    api_key: str = Field(..., description="WB API key")
    months: int = Field(3, ge=1, le=12, description="Number of months to sync (default: 3)")


class SyncReportsResponse(BaseModel):
    """Response with task ID for tracking."""
    task_id: str
    message: str


class TaskStatusResponse(BaseModel):
    """Response with task status."""
    task_id: str
    status: str  # PENDING, STARTED, PROGRESS, SUCCESS, FAILURE
    progress: Optional[dict] = None
    result: Optional[dict] = None
    error: Optional[str] = None


# ===================
# Endpoints
# ===================


@router.post("/sync", response_model=SyncReportsResponse)
async def start_sync_reports(request: SyncReportsRequest):
    """
    Start WB finance sync for N months via JSON API.
    
    Downloads weekly finance data and inserts into fact_finances.
    **Typical duration**: 30-120 minutes for 6 months of data.
    
    Use the returned task_id to check progress via GET /status/{task_id}
    """
    days_back = request.months * 30
    
    task = sync_wb_finance_history.delay(
        shop_id=request.shop_id,
        api_key=request.api_key,
        days_back=days_back,
    )
    
    return SyncReportsResponse(
        task_id=task.id,
        message=f"Started finance sync for {request.months} months (~{days_back} days).",
    )


@router.get("/status/{task_id}", response_model=TaskStatusResponse)
async def get_task_status(task_id: str):
    """
    Get the status of a download/sync task.
    
    Status values:
    - PENDING: Task is waiting to be executed
    - STARTED: Task has started
    - PROGRESS: Task is in progress (check progress field)
    - SUCCESS: Task completed successfully (check result field)
    - FAILURE: Task failed (check error field)
    """
    result = AsyncResult(task_id)
    
    response = TaskStatusResponse(
        task_id=task_id,
        status=result.status,
    )
    
    if result.status == "PROGRESS":
        response.progress = result.info
    elif result.status == "SUCCESS":
        response.result = result.result
    elif result.status == "FAILURE":
        response.error = str(result.result) if result.result else "Unknown error"
    
    return response


@router.get("/list")
async def list_downloaded_reports(
    shop_id: Optional[int] = Query(None, description="Filter by shop ID"),
    limit: int = Query(50, ge=1, le=200),
):
    """
    List downloaded report files.
    
    Returns information about files stored in /app/data/wb_reports/
    """
    import os
    from pathlib import Path
    
    reports_dir = Path("/app/data/wb_reports")
    
    if not reports_dir.exists():
        return {"reports": [], "total": 0}
    
    files = []
    for f in reports_dir.glob("wb_report_*.csv"):
        # Parse filename: wb_report_{shop_id}_{report_id}_{timestamp}.csv
        parts = f.stem.split("_")
        if len(parts) >= 4:
            file_shop_id = int(parts[2]) if parts[2].isdigit() else None
            
            # Filter by shop_id if specified
            if shop_id and file_shop_id != shop_id:
                continue
            
            files.append({
                "filename": f.name,
                "shop_id": file_shop_id,
                "report_id": parts[3] if len(parts) > 3 else None,
                "size_bytes": f.stat().st_size,
                "created_at": f.stat().st_mtime,
            })
    
    # Sort by creation time, newest first
    files.sort(key=lambda x: x["created_at"], reverse=True)
    
    return {
        "reports": files[:limit],
        "total": len(files),
    }


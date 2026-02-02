
from fastapi import APIRouter
from pydantic import BaseModel, Field
from celery.result import AsyncResult
from typing import Optional

from celery_app.tasks.tasks import sync_wb_advert_history

router = APIRouter(prefix="/advertising", tags=["Advertising"])

class SyncAdvertRequest(BaseModel):
    shop_id: int = Field(..., description="Shop ID")
    api_key: str = Field(..., description="WB Promotion API Key (Header Authorization)")
    days_back: int = Field(180, description="History depth in days (default 180)")

class TaskResponse(BaseModel):
    task_id: str
    message: str

class TaskStatusResponse(BaseModel):
    task_id: str
    status: str
    progress: Optional[dict] = None
    result: Optional[dict] = None
    error: Optional[str] = None

@router.post("/sync", response_model=TaskResponse)
async def start_advert_sync(request: SyncAdvertRequest):
    """
    Start Advertising Sync (History).
    
    Loads campaigns and daily stats for the last N days.
    """
    task = sync_wb_advert_history.delay(
        shop_id=request.shop_id,
        api_key=request.api_key,
        days_back=request.days_back
    )
    
    return TaskResponse(
        task_id=task.id,
        message=f"Started advertising sync for last {request.days_back} days."
    )

@router.get("/status/{task_id}", response_model=TaskStatusResponse)
async def get_task_status(task_id: str):
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

"""
Commercial Monitoring API endpoints.

Provides manual triggers for data sync and analytics queries.
"""
from fastapi import APIRouter
from pydantic import BaseModel, Field
from celery.result import AsyncResult
from typing import Optional

from celery_app.tasks.tasks import (
    sync_commercial_data,
    sync_warehouses,
    sync_product_content,
)

router = APIRouter(prefix="/commercial", tags=["Commercial Monitoring"])


# ============ Request/Response Models ============

class CommercialSyncRequest(BaseModel):
    shop_id: int = Field(..., description="Shop ID")
    api_key: str = Field(..., description="WB API Key (Header Authorization)")


class TaskResponse(BaseModel):
    task_id: str
    message: str


class TaskStatusResponse(BaseModel):
    task_id: str
    status: str
    progress: Optional[dict] = None
    result: Optional[dict] = None
    error: Optional[str] = None


class TurnoverRequest(BaseModel):
    shop_id: int = Field(..., description="Shop ID")
    nm_ids: Optional[list[int]] = Field(None, description="Filter by nmIds (optional)")
    days: int = Field(30, description="Period for avg daily sales (default 30)")


# ============ Endpoints ============

@router.post("/sync", response_model=TaskResponse)
async def start_commercial_sync(request: CommercialSyncRequest):
    """
    Start commercial data sync (prices + stocks + events).
    
    This task fetches current prices and stock levels,
    detects PRICE_CHANGE / STOCK_OUT / STOCK_REPLENISH events,
    and inserts snapshot data into ClickHouse.
    
    Runs automatically every 30 minutes via Celery Beat.
    """
    task = sync_commercial_data.delay(
        shop_id=request.shop_id,
        api_key=request.api_key,
    )
    return TaskResponse(
        task_id=task.id,
        message="Started commercial sync (prices + stocks).",
    )


@router.post("/sync-warehouses", response_model=TaskResponse)
async def start_warehouse_sync(request: CommercialSyncRequest):
    """
    Start warehouse dictionary sync.
    
    Fetches all WB offices and updates dim_warehouses.
    Runs automatically once per day at 4:00 AM.
    """
    task = sync_warehouses.delay(
        shop_id=request.shop_id,
        api_key=request.api_key,
    )
    return TaskResponse(
        task_id=task.id,
        message="Started warehouse dictionary sync.",
    )


@router.post("/sync-content", response_model=TaskResponse)
async def start_content_sync(request: CommercialSyncRequest):
    """
    Start product content sync (titles, photos, dimensions).
    
    Fetches product cards and detects CONTENT_CHANGE events.
    Runs automatically once per day at 4:30 AM.
    """
    task = sync_product_content.delay(
        shop_id=request.shop_id,
        api_key=request.api_key,
    )
    return TaskResponse(
        task_id=task.id,
        message="Started product content sync.",
    )


@router.get("/status/{task_id}", response_model=TaskStatusResponse)
async def get_commercial_task_status(task_id: str):
    """Get the status of a commercial sync task."""
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


@router.post("/turnover")
async def calculate_turnover(request: TurnoverRequest):
    """
    Calculate inventory turnover on demand.
    
    Formula: turnover_days = current_quantity / avg_daily_sales
    Uses ClickHouse fact_inventory_snapshot for quantities
    and fact_finances for sales data.
    
    Returns turnover data per product.
    """
    from app.core.clickhouse import get_clickhouse_client

    try:
        ch_client = get_clickhouse_client()

        # Get latest stock snapshot per product
        stock_query = """
            SELECT nm_id, sum(quantity) as total_quantity
            FROM mms_analytics.fact_inventory_snapshot
            WHERE shop_id = %(shop_id)s
              AND fetched_at = (
                  SELECT max(fetched_at)
                  FROM mms_analytics.fact_inventory_snapshot
                  WHERE shop_id = %(shop_id)s
              )
            GROUP BY nm_id
        """

        # Get average daily sales from finances
        sales_query = """
            SELECT nm_id,
                   sum(quantity) / %(days)s as avg_daily_sales
            FROM mms_analytics.fact_finances
            WHERE shop_id = %(shop_id)s
              AND order_dt >= today() - %(days)s
              AND supplier_oper_name = 'Продажа'
            GROUP BY nm_id
        """

        params = {
            "shop_id": request.shop_id,
            "days": request.days,
        }

        # Execute queries
        stocks = ch_client.query(stock_query, parameters=params).result_rows
        sales = ch_client.query(sales_query, parameters=params).result_rows
        ch_client.close()

        # Build lookup
        sales_map = {row[0]: float(row[1]) for row in sales}
        
        results = []
        for nm_id, quantity in stocks:
            if request.nm_ids and nm_id not in request.nm_ids:
                continue
                
            avg_sales = sales_map.get(nm_id, 0)
            turnover_days = round(quantity / avg_sales, 1) if avg_sales > 0 else None

            results.append({
                "nm_id": nm_id,
                "current_quantity": quantity,
                "avg_daily_sales": round(avg_sales, 2),
                "turnover_days": turnover_days,
            })

        # Sort by turnover (lowest first = most urgent)
        results.sort(key=lambda x: x["turnover_days"] or 999999)

        return {
            "shop_id": request.shop_id,
            "period_days": request.days,
            "products": results,
            "total_products": len(results),
        }
    except Exception as e:
        return {"error": str(e), "detail": "ClickHouse query failed"}

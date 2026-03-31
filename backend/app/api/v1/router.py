"""API v1 router."""

from fastapi import APIRouter

from app.api.v1.auth import router as auth_router
from app.api.v1.shops import router as shops_router
from app.api.v1.finance_reports import router as finance_reports_router
from app.api.v1.advertising import router as advertising_router
from app.api.v1.commercial import router as commercial_router
from app.api.v1.dashboard import router as dashboard_router
from app.api.v1.products import router as products_router
from app.api.v1.wb_products import router as wb_products_router
from app.api.v1.finances import router as finances_router
from app.api.v1.sales import router as sales_router
from app.api.v1.ltv import router as ltv_router
from app.api.v1.wb_ltv import router as wb_ltv_router
from app.api.v1.events import router as events_router
from app.api.v1.events_graph import router as events_graph_router
from app.api.v1.events_analysis import router as events_analysis_router
from app.api.v1.warehouses import router as warehouses_router
from app.api.v1.finances_export import router as finances_export_router
from app.api.v1.advertising_analytics import router as advertising_analytics_router
from app.api.v1.campaign_details import router as campaign_details_router
from app.api.v1.campaign_ai_analysis import router as campaign_ai_router
from app.api.v1.ad_management import router as ad_management_router

api_router = APIRouter()

# Auth & Shops
api_router.include_router(auth_router)
api_router.include_router(shops_router)

# Business logic
api_router.include_router(finance_reports_router)
api_router.include_router(finances_export_router)
api_router.include_router(advertising_router)
api_router.include_router(advertising_analytics_router)
api_router.include_router(campaign_details_router)
api_router.include_router(campaign_ai_router)
api_router.include_router(commercial_router)
api_router.include_router(dashboard_router)
api_router.include_router(products_router)
api_router.include_router(wb_products_router)
api_router.include_router(finances_router)
api_router.include_router(sales_router)
api_router.include_router(warehouses_router)
api_router.include_router(ltv_router)
api_router.include_router(wb_ltv_router)
api_router.include_router(events_router)
api_router.include_router(events_graph_router)
api_router.include_router(events_analysis_router)
api_router.include_router(ad_management_router)


@api_router.get("/")
async def root():
    """Root endpoint."""
    return {"message": "MMS API v1", "status": "operational"}


@api_router.get("/status")
async def status():
    """API status endpoint."""
    return {
        "api_version": "v1",
        "status": "healthy",
        "services": {
            "database": "connected",
            "clickhouse": "connected",
            "redis": "connected",
        },
    }

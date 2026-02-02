"""API v1 router."""

from fastapi import APIRouter

from app.api.v1.finance_reports import router as finance_reports_router
from app.api.v1.advertising import router as advertising_router

api_router = APIRouter()

# Include sub-routers
api_router.include_router(finance_reports_router)
api_router.include_router(advertising_router)


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


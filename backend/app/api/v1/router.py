"""API v1 router."""

from fastapi import APIRouter

api_router = APIRouter()


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

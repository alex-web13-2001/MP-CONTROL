"""
Shop management API endpoints.

GET    /shops     — List user's shops
POST   /shops     — Add a new shop (API key is encrypted)
DELETE /shops/{id} — Remove a shop
"""
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.core.encryption import encrypt_api_key
from app.core.security import get_current_user
from app.models.shop import Shop
from app.models.user import User
from app.schemas.auth import ShopCreate, ShopResponse

router = APIRouter(prefix="/shops", tags=["shops"])


@router.get("", response_model=list[ShopResponse])
async def list_shops(current_user: User = Depends(get_current_user)):
    """List all shops belonging to the current user."""
    return [ShopResponse.model_validate(shop) for shop in current_user.shops]


@router.post("", response_model=ShopResponse, status_code=status.HTTP_201_CREATED)
async def create_shop(
    body: ShopCreate,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Add a new marketplace shop connection."""
    # Validate Ozon requires client_id
    if body.marketplace == "ozon" and not body.client_id:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Для Ozon необходим Client-Id",
        )

    shop = Shop(
        user_id=current_user.id,
        name=body.name,
        marketplace=body.marketplace,
        api_key_encrypted=encrypt_api_key(body.api_key),
        client_id=body.client_id,
    )
    db.add(shop)
    await db.flush()
    await db.refresh(shop)

    return ShopResponse.model_validate(shop)


@router.delete("/{shop_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_shop(
    shop_id: int,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Remove a shop connection."""
    result = await db.execute(
        select(Shop).where(Shop.id == shop_id, Shop.user_id == current_user.id)
    )
    shop = result.scalar_one_or_none()

    if not shop:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Магазин не найден",
        )

    await db.delete(shop)

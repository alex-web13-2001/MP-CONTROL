import logging
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert
import datetime

from app.models.dim_ozon_campaigns import DimOzonCampaign, DimOzonCampaignProduct
from app.services.ozon_ads_service import OzonAdsService

logger = logging.getLogger(__name__)

class OzonCampaignsLoader:
    def __init__(self, db: AsyncSession, shop_id: int, ozon_ads_service: OzonAdsService):
        self.db = db
        self.shop_id = shop_id
        self.ozon_ads_service = ozon_ads_service

    async def sync_all_campaigns(self):
        """
        Загружает все кампании по магазину и обновляет справочник PostgreSQL.
        """
        logger.info(f"OzonCampaignsLoader: Starting sync for shop_id={self.shop_id}")
        
        campaigns = await self.ozon_ads_service.get_campaigns()
        if not campaigns:
            logger.info(f"OzonCampaignsLoader: No campaigns found for shop_id={self.shop_id}")
            return
            
        campaigns_values = []
        campaign_ids = []
        
        for camp in campaigns:
            camp_id = camp.get("id")
            if not camp_id:
                continue
                
            daily_budget = float(camp.get("dailyBudget", 0) or 0)
            # Иногда Ozon возвращает микрорубли. Если значение огромно, переводим в рубли.
            if daily_budget > 100000:
                daily_budget = daily_budget / 1_000_000.0

            # --- Формируем понятный тип кампании (вместо просто 'SKU') ---
            obj_type = str(camp.get("advObjectType", ""))
            if obj_type == "SKU":
                placements = camp.get("placement", [])
                mode = camp.get("productCampaignMode", "")
                payment = camp.get("PaymentType", camp.get("paymentType", ""))
                
                # Places — Ozon API иногда возвращает PLACEMENT_TOP_PROMOTION 
                # для обычных CPC кампаний, поэтому для CPC ставим «Поиск и рекомендации»
                # если нет явного PLACEMENT_SEARCH_AND_CATEGORY
                places = []
                if "PLACEMENT_SEARCH_AND_CATEGORY" in placements:
                    places.append("Поиск и рекомендации")
                if "PLACEMENT_PDP" in placements:
                    places.append("Карточка товара")
                if "PLACEMENT_TOP_PROMOTION" in placements:
                    # Для CPC кампаний PLACEMENT_TOP_PROMOTION часто некорректен
                    if payment != "CPC" or "PLACEMENT_SEARCH_AND_CATEGORY" in placements:
                        places.append("Продвижение в топ")
                
                if not places:
                    # По умолчанию для CPC — Поиск и рекомендации (самый частый тип)
                    if payment == "CPC":
                        places_str = "Поиск и рекомендации"
                    else:
                        places_str = "Везде"
                else:
                    places_str = " + ".join(places)
                    
                # Strategy
                str_type = f"Автостратегия ({places_str})"
                if mode == "PRODUCT_CAMPAIGN_MODE_AUTO":
                    if payment == "CPC":
                        str_type = f"Средняя стоимость клика ({places_str})"
                    elif payment == "CPM":
                        str_type = f"Максимальный охват ({places_str})"
                elif mode == "PRODUCT_CAMPAIGN_MODE_MANUAL":
                    if payment == "CPC":
                        str_type = f"Фикс. цена - Клики ({places_str})"
                    else:
                        str_type = f"Фикс. цена - Показы ({places_str})"
            else:
                str_type = {
                    "BANNER": "Баннер",
                    "BRAND_SHELF": "Брендовая полка",
                    "SEARCH_PROMO": "Поиск (old)",
                }.get(obj_type, obj_type)

            campaigns_values.append({
                "shop_id": self.shop_id,
                "campaign_id": int(camp_id),
                "title": str(camp.get("title", "")),
                "campaign_type": str_type,
                "state": str(camp.get("state", "")),
                "daily_budget": daily_budget,
                "payment_type": str(camp.get("PaymentType", camp.get("paymentType", ""))),
                "synced_at": datetime.datetime.now(datetime.timezone.utc),
            })
            
        if not campaigns_values:
            return

        # Дедупликация по campaign_id на случай дублей из API
        unique_campaigns = {v['campaign_id']: v for v in campaigns_values}.values()
        campaigns_values = list(unique_campaigns)

        # Делаем UPSERT кампаний
        stmt = insert(DimOzonCampaign).values(campaigns_values)
        stmt = stmt.on_conflict_do_update(
            index_elements=['shop_id', 'campaign_id'],
            set_={
                'title': stmt.excluded.title,
                'campaign_type': stmt.excluded.campaign_type,
                'state': stmt.excluded.state,
                'daily_budget': stmt.excluded.daily_budget,
                'payment_type': stmt.excluded.payment_type,
                'synced_at': stmt.excluded.synced_at,
            }
        )
        await self.db.execute(stmt)
        await self.db.commit()
        
        logger.info(f"OzonCampaignsLoader: Synced {len(campaigns_values)} campaigns for shop_id={self.shop_id}")
        
        # Загружаем товары ТОЛЬКО для активных кампаний
        active_campaign_ids = [
            int(c.get("id")) for c in campaigns 
            if c.get("id") and c.get("state") in ["CAMPAIGN_STATE_RUNNING", "CAMPAIGN_STATE_PLANNED"]
               and c.get("advObjectType") == "SKU"
        ]
        
        for camp_id in active_campaign_ids:
            try:
                await self.sync_campaign_products(camp_id)
            except Exception as e:
                logger.error(f"Error syncing products for campaign {camp_id}: {e}")
                
        logger.info(f"OzonCampaignsLoader: Sync completed for shop_id={self.shop_id}")

    async def sync_campaign_products(self, campaign_id: int):
        """
        Загружает товары (SKU и ставки) для конкретной кампании и сохраняет их.
        """
        products = await self.ozon_ads_service.get_campaign_products(campaign_id)
        if products is None:
            # Ошибка API
            return
            
        products_values = []
        for p in products:
            sku = p.get("sku")
            bid = float(p.get("bid", 0) or 0)  # Ставки всегда в микрорублях
            
            if not sku:
                continue
                
            if bid > 0:
                bid = bid / 1_000_000.0
                
            products_values.append({
                "shop_id": self.shop_id,
                "campaign_id": campaign_id,
                "sku": int(sku),
                "bid": bid,
            })
            
        if not products_values:
            # Если в кампании нет товаров, можно очистить старые записи для этой кампании
            await self.db.execute(
                DimOzonCampaignProduct.__table__.delete().where(
                    DimOzonCampaignProduct.shop_id == self.shop_id,
                    DimOzonCampaignProduct.campaign_id == campaign_id
                )
            )
            await self.db.commit()
            return
            
        # Дедупликация по sku
        unique_products = {v['sku']: v for v in products_values}.values()
        products_values = list(unique_products)

        # UPSERT SKU и их ставок
        stmt = insert(DimOzonCampaignProduct).values(products_values)
        stmt = stmt.on_conflict_do_update(
            index_elements=['shop_id', 'campaign_id', 'sku'],
            set_={
                'bid': stmt.excluded.bid,
            }
        )
        await self.db.execute(stmt)
        
        # Удалим те SKU из БД, которых больше нет в API Ozon для этой кампании
        current_skus = [p["sku"] for p in products_values]
        delete_stmt = DimOzonCampaignProduct.__table__.delete().where(
            DimOzonCampaignProduct.shop_id == self.shop_id,
            DimOzonCampaignProduct.campaign_id == campaign_id,
            DimOzonCampaignProduct.sku.not_in(current_skus)
        )
        await self.db.execute(delete_stmt)
        
        await self.db.commit()

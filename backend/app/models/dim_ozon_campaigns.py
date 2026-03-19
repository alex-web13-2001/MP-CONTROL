from sqlalchemy import Column, Integer, BigInteger, String, Numeric, DateTime, ForeignKey, UniqueConstraint, ForeignKeyConstraint, func
from sqlalchemy.orm import relationship
from app.core.database import Base

class DimOzonCampaign(Base):
    """
    Справочник рекламных кампаний Ozon (performance).
    Мастер-данные для управления кампаниями и автобиддера.
    """
    __tablename__ = "dim_ozon_campaigns"
    
    id = Column(Integer, primary_key=True, index=True)
    shop_id = Column(Integer, ForeignKey("shops.id", ondelete="CASCADE"), nullable=False, index=True)
    campaign_id = Column(BigInteger, nullable=False, index=True)
    title = Column(String, default="")
    campaign_type = Column(String, default="") # SKU, BANNER, SEARCH_PROMO, BRAND_SHELF
    state = Column(String, default="")         # CAMPAIGN_STATE_RUNNING, CAMPAIGN_STATE_INACTIVE, etc.
    daily_budget = Column(Numeric(12, 2), default=0)
    payment_type = Column(String, default="")  # CPC, CPM
    
    synced_at = Column(DateTime(timezone=True), default=func.now())
    created_at = Column(DateTime(timezone=True), default=func.now())
    updated_at = Column(DateTime(timezone=True), default=func.now(), onupdate=func.now())
    
    # Cascade deletes: if shop is deleted, campaigns are deleted
    shop = relationship("Shop", backref="ozon_campaigns")
    
    __table_args__ = (
        UniqueConstraint('shop_id', 'campaign_id', name='uq_ozon_campaign_shop'),
    )


class DimOzonCampaignProduct(Base):
    """
    Связь товаров в кампаниях Ozon и их индивидуальные ставки.
    """
    __tablename__ = "dim_ozon_campaign_products"
    
    id = Column(Integer, primary_key=True, index=True)
    shop_id = Column(Integer, ForeignKey("shops.id", ondelete="CASCADE"), nullable=False, index=True)
    campaign_id = Column(BigInteger, nullable=False, index=True)
    sku = Column(BigInteger, nullable=False, index=True)
    bid = Column(Numeric(10, 2), default=0)
    
    updated_at = Column(DateTime(timezone=True), default=func.now(), onupdate=func.now())
    
    __table_args__ = (
        ForeignKeyConstraint(
            ['shop_id', 'campaign_id'], 
            ['dim_ozon_campaigns.shop_id', 'dim_ozon_campaigns.campaign_id'], 
            ondelete='CASCADE',
            name='fk_ozon_campaign_products_campaign'
        ),
        UniqueConstraint('shop_id', 'campaign_id', 'sku', name='uq_ozon_camp_prod_sku'),
    )

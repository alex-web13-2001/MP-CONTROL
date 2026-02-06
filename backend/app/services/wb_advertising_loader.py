
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import List, Dict, Any, Optional
import logging

import clickhouse_connect
from clickhouse_connect.driver.client import Client as ClickHouseClient

logger = logging.getLogger(__name__)

@dataclass
class DimAdvertCampaignRow:
    shop_id: int
    advert_id: int
    name: str
    type: int
    status: int
    updated_at: datetime = None
    
    def __post_init__(self):
        if self.updated_at is None:
            self.updated_at = datetime.now()

@dataclass
class FactAdvertStatsV3Row:
    """Row for fact_advert_stats_v3 with full funnel metrics."""
    date: date
    shop_id: int
    advert_id: int
    nm_id: int
    views: int
    clicks: int
    atbs: int        # Корзины
    orders: int      # Заказы
    revenue: Decimal # Выручка (sum_price)
    spend: Decimal   # Затраты (sum)
    updated_at: datetime = None
    
    def __post_init__(self):
        if self.updated_at is None:
            self.updated_at = datetime.now()

class WBAdvertisingLoader:
    """
    Loader for WB Advertising data into ClickHouse.
    Updated for V3 API with funnel metrics.
    """
    
    DB_NAME = "mms_analytics"
    TABLE_DIM = "dim_advert_campaigns"
    TABLE_FACT_V3 = "fact_advert_stats_v3"

    def __init__(self, 
                 host: str = "clickhouse", 
                 port: int = 8123, 
                 username: str = "default", 
                 password: str = "",
                 database: str = "mms_analytics"):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.database = database
        self._client: Optional[ClickHouseClient] = None

    def connect(self):
        self._client = clickhouse_connect.get_client(
            host=self.host, port=self.port, username=self.username, password=self.password, database=self.database
        )

    def close(self):
        if self._client:
            self._client.close()
            self._client = None

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def load_campaigns(self, campaigns: List[Dict[str, Any]], shop_id: int):
        """
        Load campaigns into dim_advert_campaigns.
        """
        rows = []
        for c in campaigns:
            rows.append((
                shop_id,
                int(c.get("advertId", 0)),
                str(c.get("name", "")),
                int(c.get("type", 0)),
                int(c.get("status", 0)),
                datetime.now()
            ))
        
        if rows and self._client:
            self._client.insert(
                f"{self.DB_NAME}.{self.TABLE_DIM}",
                rows,
                column_names=["shop_id", "advert_id", "name", "type", "status", "updated_at"]
            )
        return len(rows)

    def parse_full_stats_v3(self, full_stats: List[Dict[str, Any]], shop_id: int) -> List[FactAdvertStatsV3Row]:
        """
        Parse /adv/v3/fullstats response.
        
        CRITICAL FIX:
        The API returns multiple entries for the same (date, advert, nm) split by 'appType' (Android, iOS, Web).
        Since ClickHouse table is ReplacingMergeTree order by (shop, nm, date, advert), 
        inserting multiple rows for the same key causes deduplication and DATA LOSS.
        
        We MUST aggregate (SUM) the metrics in Python before returning rows.
        """
        rows = []
        logger.info(f"Parsing V3 fullstats: {len(full_stats)} campaigns")
        
        # Aggregation dictionary: (date, advert_id, nm_id) -> {metrics}
        aggregated_data = {}
        
        for campaign in full_stats:
            advert_id = int(campaign.get("advertId", 0))
            days = campaign.get("days", [])
            
            if not days:
                continue
            
            for d in days:
                date_str = d.get("date", "")
                try:
                    # Handle ISO format: "2026-01-28T00:00:00Z"
                    event_date = datetime.fromisoformat(date_str.replace("Z", "+00:00")).date()
                except ValueError:
                    try:
                        event_date = datetime.strptime(date_str.split("T")[0], "%Y-%m-%d").date()
                    except ValueError:
                        logger.warning(f"Could not parse date: {date_str}")
                        continue

                apps = d.get("apps", [])
                found_nms = False
                
                # 1. Collect NM level stats
                for app in apps:
                    nms_list = app.get("nms", [])
                    for nm in nms_list:
                        found_nms = True
                        nm_id = int(nm.get("nmId", 0))
                        key = (event_date, advert_id, nm_id)
                        
                        if key not in aggregated_data:
                            aggregated_data[key] = {
                                "views": 0, "clicks": 0, "atbs": 0, "orders": 0,
                                "revenue": Decimal(0), "spend": Decimal(0)
                            }
                        
                        stats = aggregated_data[key]
                        stats["views"] += int(nm.get("views", 0))
                        stats["clicks"] += int(nm.get("clicks", 0))
                        stats["atbs"] += int(nm.get("atbs", 0))
                        stats["orders"] += int(nm.get("orders", 0))
                        stats["revenue"] += Decimal(str(nm.get("sum_price", 0)))
                        stats["spend"] += Decimal(str(nm.get("sum", 0)))

                # 2. Fallback: If no NMs found, use day-level aggregates (nm_id=0)
                if not found_nms:
                    views = d.get("views", 0)
                    clicks = d.get("clicks", 0)
                    spend = d.get("sum", 0)
                    if views or clicks or spend:
                         key = (event_date, advert_id, 0)
                         if key not in aggregated_data:
                            aggregated_data[key] = {
                                "views": 0, "clicks": 0, "atbs": 0, "orders": 0,
                                "revenue": Decimal(0), "spend": Decimal(0)
                            }
                         stats = aggregated_data[key]
                         stats["views"] += int(views)
                         stats["clicks"] += int(clicks)
                         stats["atbs"] += int(d.get("atbs", 0))
                         stats["orders"] += int(d.get("orders", 0))
                         stats["revenue"] += Decimal(str(d.get("sum_price", 0)))
                         stats["spend"] += Decimal(str(spend))

        # Convert aggregated dict to rows
        for (date_val, advert_id, nm_id), stats in aggregated_data.items():
            rows.append(FactAdvertStatsV3Row(
                date=date_val,
                shop_id=shop_id,
                advert_id=advert_id,
                nm_id=nm_id,
                views=stats["views"],
                clicks=stats["clicks"],
                atbs=stats["atbs"],
                orders=stats["orders"],
                revenue=stats["revenue"],
                spend=stats["spend"]
            ))
        
        logger.info(f"Parsed {len(rows)} aggregated V3 stats rows")
        return rows

    def insert_stats_v3(self, rows: List[FactAdvertStatsV3Row]):
        """Insert rows into fact_advert_stats_v3."""
        if not rows or not self._client:
            return 0
            
        data = [
            (
                r.date, r.shop_id, r.advert_id, r.nm_id,
                r.views, r.clicks, r.atbs, r.orders,
                float(r.revenue), float(r.spend), r.updated_at
            )
            for r in rows
        ]
        
        self._client.insert(
            f"{self.DB_NAME}.{self.TABLE_FACT_V3}",
            data,
            column_names=["date", "shop_id", "advert_id", "nm_id", "views", "clicks", "atbs", "orders", "revenue", "spend", "updated_at"]
        )
        return len(data)

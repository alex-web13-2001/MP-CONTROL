
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import List, Dict, Any, Optional

import clickhouse_connect
from clickhouse_connect.driver.client import Client as ClickHouseClient

@dataclass
class DimAdvertCampaignRow:
    shop_id: int
    advert_id: int
    name: str
    type: int
    status: int
    updated_at: datetime = datetime.now()

@dataclass
class FactAdvertStatsRow:
    date: date
    shop_id: int
    advert_id: int
    nm_id: int
    views: int
    clicks: int
    spend: Decimal
    ctr: float
    cpc: Decimal
    updated_at: datetime = datetime.now()

class WBAdvertisingLoader:
    """
    Loader for WB Advertising data into ClickHouse.
    """
    
    DB_NAME = "mms_analytics"
    TABLE_DIM = "dim_advert_campaigns"
    TABLE_FACT = "fact_advert_stats"

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
        Expected API structure from /adv/v1/promotion/adverts:
        [ { "advertId": 123, "name": "Camaign", "type": 8, "status": 9, ... }, ... ]
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

    def parse_full_stats(self, full_stats: List[Dict[str, Any]], shop_id: int) -> List[FactAdvertStatsRow]:
        """
        Parse /adv/v2/fullstats response.
        Structure:
        [
          {
            "id": 123,
            "days": [
               {
                 "date": "2024-01-01T00:00:00Z",
                 "apps": [...],
                 "nm": [
                    { "nmId": 999, "views": 10, "clicks": 1, "sum": 50.0, "ctr": 10.0, "cpc": 50.0 }
                 ]
               }
            ]
          }
        ]
        """
        rows = []
        for campaign in full_stats:
            advert_id = int(campaign.get("advertId", 0) or campaign.get("id", 0))
            days = campaign.get("days", [])
            if not days:
                continue
            
            for d in days:
                date_str = d.get("date", "") # 2024-01-01T00:00:00Z
                try:
                    event_date = datetime.strptime(date_str.split("T")[0], "%Y-%m-%d").date()
                except ValueError:
                    continue

                # We care about 'nm' (stats by product)
                nm_stats = d.get("nm", [])
                for nm in nm_stats:
                    rows.append(FactAdvertStatsRow(
                        date=event_date,
                        shop_id=shop_id,
                        advert_id=advert_id,
                        nm_id=int(nm.get("nmId", 0)),
                        views=int(nm.get("views", 0)),
                        clicks=int(nm.get("clicks", 0)),
                        spend=Decimal(str(nm.get("sum", 0))),
                        ctr=float(nm.get("ctr", 0)),
                        cpc=Decimal(str(nm.get("cpc", 0)))
                    ))
        return rows

    def insert_stats(self, rows: List[FactAdvertStatsRow]):
        if not rows or not self._client:
            return 0
            
        data = [
            (
                r.date, r.shop_id, r.advert_id, r.nm_id, 
                r.views, r.clicks, float(r.spend), r.ctr, float(r.cpc), r.updated_at
            )
            for r in rows
        ]
        
        self._client.insert(
            f"{self.DB_NAME}.{self.TABLE_FACT}",
            data,
            column_names=["date", "shop_id", "advert_id", "nm_id", "views", "clicks", "spend", "ctr", "cpc", "updated_at"]
        )
        return len(data)

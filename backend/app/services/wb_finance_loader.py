"""
WB Finance Data Loader - Parse JSON API data and load into ClickHouse fact_finances.

This service:
1. Parses WB V5 API JSON responses (reportDetailByPeriod)
2. Maps API fields to fact_finances schema
3. Batch inserts data into ClickHouse
"""

import json
import logging
from dataclasses import dataclass
from datetime import datetime, date, timedelta
from decimal import Decimal
from typing import Any, Dict, List, Optional, Generator

logger = logging.getLogger(__name__)

import clickhouse_connect
from clickhouse_connect.driver.client import Client as ClickHouseClient


@dataclass
class FactFinancesRow:
    """Single row for fact_finances table."""
    event_date: date
    shop_id: int
    marketplace: str  # 'wb' or 'ozon'
    order_id: str
    external_id: str  # nmId
    vendor_code: str  # sa_name
    rrd_id: int       # Unique report line ID
    operation_type: str
    quantity: int
    retail_amount: Decimal
    payout_amount: Decimal
    # Geography
    warehouse_name: str
    delivery_address: str
    region_name: str
    
    # Detailed Expenses
    commission_amount: float
    logistics_total: float
    ads_total: float
    penalty_total: float
    storage_fee: float = 0.0
    acceptance_fee: float = 0.0
    bonus_amount: float = 0.0
    
    # Identifiers
    shk_id: str = ""
    rid: str = ""
    srid: str = ""

    # Specifics
    wb_gi_id: int = 0
    wb_ppvz_for_pay: Decimal = Decimal("0")
    wb_delivery_rub: Decimal = Decimal("0")
    wb_storage_amount: Decimal = Decimal("0")
    # Service
    source_file_name: str = ""
    raw_payload: str = ""


class WBReportParser:
    """
    Parser for WB V5 API JSON responses.
    
    Field Mapping (API → fact_finances):
    - event_date        ← rr_dt or sale_dt
    - order_id          ← srid (unique sale ID)
    - external_id       ← nm_id
    - vendor_code       ← sa_name
    - operation_type    ← supplier_oper_name
    - payout_amount     ← ppvz_for_pay
    - commission_amount ← ppvz_sales_commission * -1
    - logistics_total   ← delivery_rub + rebill_logistic_cost
    """
    
    def __init__(self, shop_id: int):
        self.shop_id = shop_id
    
    def _safe_decimal(self, value: Any, default: Decimal = Decimal("0")) -> Decimal:
        """Safely convert value to Decimal."""
        if value is None or value == "":
            return default
        try:
            return Decimal(str(value))
        except Exception:
            return default
    
    def _safe_int(self, value: Any, default: int = 0) -> int:
        """Safely convert value to int."""
        if value is None or value == "":
            return default
        try:
            # Handle float strings like "123.0"
            return int(float(str(value)))
        except Exception:
            return default

    def _safe_float(self, value: Any, default: float = 0.0) -> float:
        """Safely convert value to float."""
        if value is None or value == "":
            return default
        try:
            return float(str(value))
        except Exception:
            return default
    
    def _parse_date(self, value: str) -> Optional[date]:
        """Parse date from various formats."""
        if not value:
            return None
        
        # Try different formats
        formats = [
            "%Y-%m-%d",
            "%Y-%m-%dT%H:%M:%SZ",
            "%Y-%m-%dT%H:%M:%S",
            "%d.%m.%Y",
        ]
        
        for fmt in formats:
            try:
                return datetime.strptime(value.split("T")[0] if "T" in value else value, fmt.split("T")[0]).date()
            except ValueError:
                continue
        
        return None
    
    def parse_row(self, row: Dict[str, Any], source_filename: str) -> Optional[FactFinancesRow]:
        """Parse a single API row (dict) into FactFinancesRow."""
        # Get event date - prefer rr_dt, fallback to sale_dt
        event_date = self._parse_date(row.get("rr_dt", "")) or self._parse_date(row.get("sale_dt", ""))
        if not event_date:
            return None  # Skip rows without valid date
        
        # Extract order ID from srid
        order_id = str(row.get("srid", "") or row.get("shk_id", "") or "")
        if not order_id:
            order_id = f"{row.get('rrd_id', '')}"
        
        # External ID (nmId)
        external_id = str(row.get("nm_id", "") or "0")
        
        # Vendor code (sa_name - seller's article)
        vendor_code = str(row.get("sa_name", "") or "")
        rrd_id = self._safe_int(row.get("rrd_id", 0))
        operation_type = str(row.get("supplier_oper_name", "") or "Неизвестно")
        
        # Quantities and amounts
        quantity = self._safe_int(row.get("quantity", 0))
        retail_amount = self._safe_decimal(row.get("retail_amount", 0))
        payout_amount = self._safe_decimal(row.get("ppvz_for_pay", 0))
        
        # Geography
        warehouse_name = row.get("office_name", "") or ""
        delivery_address = row.get("ppvz_office_name", "") or ""
        region_name = row.get("gi_box_type_name", "") or "" # Often mapped to region or delivery type
        
        # Identifiers
        shk_id = str(row.get("shk_id", ""))
        rid = str(row.get("rid", ""))
        srid = str(row.get("srid", ""))
        
        # Costs
        # REVERTED: Use direct WB value for commission. 
        # API returns negative value for deduction, we store positive expense.
        commission = self._safe_float(row.get("ppvz_sales_commission", 0))
        commission_amount = abs(commission)
        # Old logic was: round(float(retail_amount) * (commission_percent / 100), 2) - INCORRECT
        
        # Direct costs fields
        # Note: storage_fee/acceptance_fee might not be in detailed report V1/V5 implicitly, 
        # often they are separate services. But if present, we map them.
        # However, for V5 'reportDetailByPeriod', fields like 'storage_fee' are usually separate reports.
        # But we map what we have or default to 0.
        # User specifically asked for 'storage_fee' mapping. If JSON has it, we take it.
        storage_fee = self._safe_float(row.get("storage_fee", 0))
        acceptance_fee = self._safe_float(row.get("acceptance", 0)) # 'acceptance' is often the field name
        bonus_amount = self._safe_float(row.get("bonus", 0)) # 'bonus_type_name'? No, monetary bonus.
        
        # Logistics from 'delivery_rub'
        logistics_base = self._safe_float(row.get("delivery_rub", 0))
        rebill_logistic = self._safe_float(row.get("rebill_logistic_cost", 0))
        logistics_total = logistics_base + rebill_logistic
        
        # Penalties
        penalty_total = abs(self._safe_float(row.get("penalty", 0)))
        
        # WB specific
        wb_gi_id = self._safe_int(row.get("gi_id", 0))
        wb_ppvz_for_pay = payout_amount
        wb_delivery_rub = self._safe_decimal(row.get("delivery_rub", 0))
        wb_storage_amount = abs(self._safe_decimal(row.get("storage_fee", 0)))
        
        return FactFinancesRow(
            event_date=event_date,
            shop_id=self.shop_id,
            marketplace="wb",
            order_id=order_id,
            external_id=external_id,
            vendor_code=vendor_code,
            rrd_id=rrd_id,
            operation_type=operation_type,
            quantity=quantity,
            retail_amount=retail_amount,
            payout_amount=payout_amount,
            # Geography
            warehouse_name=warehouse_name,
            delivery_address=delivery_address,
            region_name=region_name,
            
            # Detailed Expenses
            commission_amount=commission_amount,
            logistics_total=logistics_total,
            ads_total=0.0,  # Placeholder
            penalty_total=penalty_total,
            storage_fee=storage_fee,
            acceptance_fee=acceptance_fee,
            bonus_amount=bonus_amount,
            
            # Identifiers
            shk_id=shk_id,
            rid=rid,
            srid=srid,

            # Specifics
            wb_gi_id=wb_gi_id,
            wb_ppvz_for_pay=wb_ppvz_for_pay,
            wb_delivery_rub=wb_delivery_rub,
            wb_storage_amount=wb_storage_amount,
            source_file_name=source_filename,
            raw_payload=json.dumps(row, ensure_ascii=False, default=str),
        )

    def parse_json_rows(self, data: List[Dict[str, Any]], source_name: str = "api_json") -> Generator[FactFinancesRow, None, None]:
        """Parse list of JSON dicts from V5 API and yield FactFinancesRow objects."""
        for row in data:
            parsed = self.parse_row(row, source_name)
            if parsed:
                yield parsed


class ClickHouseLoader:
    """
    Loader for inserting data into ClickHouse fact_finances table.
    
    Uses batch inserts for performance.
    """
    
    BATCH_SIZE = 1000
    TABLE_NAME = "mms_analytics.fact_finances"
    
    COLUMNS = [
        "event_date", "shop_id", "marketplace", "order_id", "external_id",
        "vendor_code", "rrd_id", "operation_type", "quantity", "retail_amount", "payout_amount",
        # Geography
        "warehouse_name", "delivery_address", "region_name",
        # Expenses
        "commission_amount", "logistics_total", "ads_total", "penalty_total",
        "storage_fee", "acceptance_fee", "bonus_amount",
        # Identifiers
        "shk_id", "rid", "srid",
        # WB Specific
        "wb_gi_id", "wb_ppvz_for_pay", "wb_delivery_rub", "wb_storage_amount",
        # Ozon Specific
        "ozon_acquiring", "ozon_last_mile", "ozon_milestone", "ozon_marketing_services",
        # Service
        "source_file_name", "raw_payload"
    ]
    
    def __init__(
        self,
        host: str = "localhost",
        port: int = 8123,
        username: str = "default",
        password: str = "",
        database: str = "mms_analytics",
    ):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.database = database
        self._client: Optional[ClickHouseClient] = None
    
    def connect(self):
        """Establish connection to ClickHouse."""
        self._client = clickhouse_connect.get_client(
            host=self.host,
            port=self.port,
            username=self.username,
            password=self.password,
            database=self.database,
        )
    
    def close(self):
        """Close connection."""
        if self._client:
            self._client.close()
            self._client = None
    
    def __enter__(self):
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
    
    def _row_to_tuple(self, row: FactFinancesRow) -> tuple:
        """Convert FactFinancesRow to tuple for insert."""
        return (
            # Core + Money
            row.event_date,
            row.shop_id,
            row.marketplace,
            row.order_id,
            row.external_id,
            row.vendor_code,
            row.rrd_id,
            row.operation_type,
            row.quantity,
            float(row.retail_amount),
            float(row.payout_amount),
            # Geography
            row.warehouse_name,
            row.delivery_address,
            row.region_name,
            # Expenses
            float(row.commission_amount),
            float(row.logistics_total),
            float(row.ads_total),
            float(row.penalty_total),
            float(row.storage_fee),
            float(row.acceptance_fee),
            float(row.bonus_amount),
            # Identifiers
            row.shk_id,
            row.rid,
            row.srid,
            # WB Specific
            row.wb_gi_id,
            float(row.wb_ppvz_for_pay),
            float(row.wb_delivery_rub),
            float(row.wb_storage_amount),
            # Ozon Specific (defaults 0)
            0.0, # ozon_acquiring
            0.0, # ozon_last_mile
            0.0, # ozon_milestone
            0.0, # ozon_marketing_services
            # Service
            row.source_file_name,
            row.raw_payload,
        )
    
    def insert_batch(self, rows: List[FactFinancesRow]) -> int:
        """Insert a batch of rows into fact_finances."""
        if not rows:
            return 0
        
        if not self._client:
            raise RuntimeError("Not connected to ClickHouse")
        
        data = [self._row_to_tuple(r) for r in rows]
        
        self._client.insert(
            self.TABLE_NAME,
            data,
            column_names=self.COLUMNS,
        )
        
        return len(data)
    
    def load_from_generator(
        self,
        rows: Generator[FactFinancesRow, None, None],
        progress_callback: Optional[callable] = None,
    ) -> int:
        """Load rows from generator with batching."""
        batch: List[FactFinancesRow] = []
        total_inserted = 0
        
        for row in rows:
            batch.append(row)
            
            if len(batch) >= self.BATCH_SIZE:
                inserted = self.insert_batch(batch)
                total_inserted += inserted
                batch = []
                
                if progress_callback:
                    progress_callback(total_inserted)
        
        # Insert remaining rows
        if batch:
            inserted = self.insert_batch(batch)
            total_inserted += inserted
        
        return total_inserted
    
    
    def get_row_count(self, shop_id: int, date_from: date, date_to: date) -> int:
        """Get count of rows for a shop in date range."""
        if not self._client:
            raise RuntimeError("Not connected to ClickHouse")
        
        result = self._client.query(
            f"""
            SELECT count() 
            FROM {self.TABLE_NAME} 
            WHERE shop_id = {{shop_id:UInt32}} 
              AND event_date >= {{date_from:Date}} 
              AND event_date <= {{date_to:Date}}
            """,
            parameters={
                "shop_id": shop_id,
                "date_from": date_from,
                "date_to": date_to,
            }
        )
        
        return result.first_row[0] if result.first_row else 0


def generate_week_ranges(months: int = 3) -> List[tuple]:
    """
    Generate weekly date ranges for the past N months.
    
    WB reports are weekly (Mon-Sun), so we generate ranges accordingly.
    
    Args:
        months: Number of months to go back
    
    Returns:
        List of (date_from, date_to) tuples
    """
    today = date.today()
    
    # Calculate start date (N months ago)
    start_date = today - timedelta(days=months * 30)
    
    # Align to Monday
    days_since_monday = start_date.weekday()
    start_date = start_date - timedelta(days=days_since_monday)
    
    ranges = []
    current = start_date
    
    while current < today:
        week_end = current + timedelta(days=6)  # Sunday
        
        # Don't go past today
        if week_end > today:
            week_end = today
        
        ranges.append((current, week_end))
        current = week_end + timedelta(days=1)  # Next Monday
    
    return ranges

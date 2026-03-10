"""
WB Tariffs Service — Fetch acceptance coefficients & storage/delivery tariffs.

API: GET https://common-api.wildberries.ru/api/tariffs/v1/acceptance/coefficients
  Returns: acceptance coefficients + storage/delivery tariffs per warehouse for 14 days ahead.

Target: ClickHouse fact_wb_acceptance_tariffs
"""

import logging
from datetime import datetime
from typing import Any

import httpx

logger = logging.getLogger(__name__)

WB_TARIFFS_URL = "https://common-api.wildberries.ru/api/tariffs/v1/acceptance/coefficients"


class WBTariffsService:
    """Fetch WB warehouse acceptance coefficients & tariffs."""

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.headers = {"Authorization": api_key}

    async def fetch_acceptance_coefficients(self) -> list[dict[str, Any]]:
        """
        Fetch acceptance coefficients for all warehouses (14 days ahead).

        Returns:
            List of dicts with keys:
                date, coefficient, warehouseID, warehouseName, allowUnload,
                boxTypeID, storageCoef, deliveryCoef, deliveryBaseLiter,
                deliveryAdditionalLiter, storageBaseLiter, storageAdditionalLiter,
                isSortingCenter
        """
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.get(WB_TARIFFS_URL, headers=self.headers)
            resp.raise_for_status()
            data = resp.json()

        if isinstance(data, list):
            items = data
        elif isinstance(data, dict):
            items = data.get("data", data.get("response", []))
            if isinstance(items, dict):
                items = items.get("data", [])
        else:
            items = []

        logger.info("WBTariffsService: fetched %d acceptance entries", len(items))
        return items

    def prepare_ch_rows(
        self,
        items: list[dict[str, Any]],
        fetched_at: datetime | None = None,
    ) -> list[tuple]:
        """
        Convert API response to ClickHouse insert rows.

        Returns list of tuples matching fact_wb_acceptance_tariffs columns:
            (dt, warehouse_id, warehouse_name, box_type_id, coefficient,
             allow_unload, is_sorting_center,
             storage_coef, storage_base_liter, storage_additional_liter,
             delivery_coef, delivery_base_liter, delivery_additional_liter,
             updated_at)
        """
        now = fetched_at or datetime.utcnow()
        rows = []

        for item in items:
            try:
                date_str = item.get("date", "")
                if date_str:
                    dt = datetime.fromisoformat(date_str.replace("Z", "+00:00")).date()
                else:
                    continue

                warehouse_id = item.get("warehouseID", 0)
                if not warehouse_id:
                    continue

                rows.append((
                    dt,
                    warehouse_id,
                    item.get("warehouseName", ""),
                    item.get("boxTypeID", 0),
                    float(item.get("coefficient", 0)),
                    1 if item.get("allowUnload") else 0,
                    1 if item.get("isSortingCenter") else 0,
                    str(item.get("storageCoef") or ""),
                    str(item.get("storageBaseLiter") or ""),
                    str(item.get("storageAdditionalLiter") or ""),
                    str(item.get("deliveryCoef") or ""),
                    str(item.get("deliveryBaseLiter") or ""),
                    str(item.get("deliveryAdditionalLiter") or ""),
                    now,
                ))
            except Exception as e:
                logger.warning("WBTariffsService: skip item: %s", e)

        logger.info("WBTariffsService: prepared %d CH rows", len(rows))
        return rows

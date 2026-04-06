"""
Backward compatibility shim.

This file re-exports all tasks from their new locations.
All tasks have been split into sub-modules:
  - helpers.py          — dedup dispatch, signal handlers
  - coordinators.py     — sync_all_*, update_all_bids dispatchers
  - onboarding.py       — load_historical_data, sync_full_history
  - wb_sync.py          — WB orders, finance, commercial, warehouses, etc.
  - wb_advertising.py   — WB campaigns, bids, budgets, normquery
  - ozon_sync.py        — Ozon products, orders, finance, funnel, etc.
  - ozon_advertising.py — Ozon ads, campaigns, bid monitoring
  - misc.py             — sync_marketplace_data, example_task, etc.
"""

# Re-export everything for backward compatibility
from celery_app.tasks.helpers import _dedup_dispatch, _cleanup_dedup_key  # noqa: F401

from celery_app.tasks.coordinators import (  # noqa: F401
    update_all_bids,
    check_all_positions,
    sync_all_daily,
    sync_all_placement_cost,
    sync_all_frequent,
    sync_all_ads,
    sync_all_budgets,
    sync_all_campaign_snapshots,
)

from celery_app.tasks.onboarding import (  # noqa: F401
    load_historical_data,
    sync_full_history,
)

from celery_app.tasks.wb_sync import (  # noqa: F401
    sync_wb_finance_history,
    sync_commercial_data,
    sync_warehouses,
    sync_product_content,
    sync_wb_tariffs,
    sync_wb_paid_storage,
    backfill_wb_paid_storage,
    sync_sales_funnel,
    backfill_sales_funnel,
    sync_orders,
    backfill_orders,
)

from celery_app.tasks.wb_advertising import (  # noqa: F401
    update_bids,
    check_positions,
    sync_wb_budgets,
    sync_wb_campaign_snapshot,
    sync_wb_advert_history,
    sync_normquery_data,
    backfill_normquery_data,
)

from celery_app.tasks.ozon_sync import (  # noqa: F401
    sync_ozon_products,
    sync_ozon_product_snapshots,
    sync_ozon_orders,
    backfill_ozon_orders,
    sync_ozon_finance,
    backfill_ozon_finance,
    sync_ozon_funnel,
    backfill_ozon_funnel,
    sync_ozon_returns,
    backfill_ozon_returns,
    sync_ozon_warehouse_stocks,
    sync_ozon_prices,
    sync_ozon_seller_rating,
    sync_ozon_content,
    sync_ozon_inventory,
    sync_ozon_commissions,
    sync_ozon_content_rating,
    sync_ozon_turnover,
    sync_ozon_placement_cost,
    backfill_ozon_placement_cost,
)

from celery_app.tasks.ozon_advertising import (  # noqa: F401
    monitor_ozon_bids,
    sync_ozon_campaigns_task,
    sync_ozon_ad_stats,
    backfill_ozon_ads,
)

from celery_app.tasks.misc import (  # noqa: F401
    sync_marketplace_data,
    example_task,
    send_notification,
)

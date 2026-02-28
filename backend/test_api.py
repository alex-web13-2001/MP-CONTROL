import sys
import asyncio
sys.path.insert(0, '/app')
from app.db.session import SessionLocal
from app.models.user import User
from app.api.v1.finances import get_ozon_products_finance

async def main():
    db = SessionLocal()
    user = db.query(User).first()
    res = await get_ozon_products_finance(
        shop_id=17,
        period=7,
        date_from=None,
        date_to=None,
        db=db,
        current_user=user
    )
    totals = res.get("totals", {})
    t = totals.get("current", {})
    print("TOTALS REVENUE:", t.get("revenue"))
    print("TOTALS COMMISSION:", t.get("commission"))
    print("TOTALS LOGISTICS:", t.get("logistics"))
    print("TOTALS ADS:", t.get("ad_spend"))
    print("TOTALS COGS:", t.get("cogs"))
    print("TOTALS PROFIT:", t.get("profit"))

    prods = res.get("products", [])
    s_rev = sum(p["current"]["revenue"] for p in prods)
    s_log = sum(p["current"]["logistics"] for p in prods)
    s_ads = sum(p["current"]["ad_spend"] for p in prods)
    s_profit = sum(p["current"]["profit"] for p in prods)
    print("\nMANUAL SUM:")
    print("Rev:", s_rev, "Log:", s_log, "Ads:", s_ads, "Profit:", s_profit)

asyncio.run(main())

import asyncio
from datetime import date
from app.api.v1.finances import get_wb_finances
from app.core.database import async_session_maker
from app.models.shop import Shop
from sqlalchemy.future import select

class DummyUser:
    def __init__(self, uid):
        self.id = uid

async def main():
    async with async_session_maker() as db:
        res = await db.execute(select(Shop.user_id).where(Shop.id == 18))
        uid = res.scalar()
        
        res = await get_wb_finances(
            shop_id=18,
            current_user=DummyUser(uid),
            date_from=date(2026, 2, 16),
            date_to=date(2026, 2, 22),
            period=7,
            group_by='day',
            db=db
        )
        import json
        print(json.dumps(res, indent=2))

if __name__ == "__main__":
    asyncio.run(main())

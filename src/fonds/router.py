from fastapi import APIRouter, Depends
from fastapi_cache.decorator import cache
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.auth.config import current_user
from src.auth.models import User
from src.database import get_async_session
from src.fonds.models import figi as figi_table, Sectors

router = APIRouter(
    prefix="/fonds",
    tags=["Fonds"],
)


@router.get("/get_shares_by_sector")
@cache(expire=30)
async def get_shares_by_sector(
        sector: Sectors,
        session: AsyncSession = Depends(get_async_session),
        user: User = Depends(current_user)
):
    query = select(figi_table).where(figi_table.c.sector == sector.name)
    shares = await session.execute(query)
    return shares.mappings().all()


@router.get("/sectors")
@cache(expire=30)
async def get_all_sectors(session: AsyncSession = Depends(get_async_session), user: User = Depends(current_user)):
    query = select(figi_table.c.sector).distinct()
    sectors = await session.execute(query)
    return sectors.mappings().all()

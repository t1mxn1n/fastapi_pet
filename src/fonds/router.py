from fastapi import APIRouter, Depends
from fastapi_cache.decorator import cache
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from src.auth.config import current_user
from src.auth.models import User
from src.database import get_async_session
from src.fonds.models import figi as figi_table, Sectors
from src.fonds.utils import fundamentals

router = APIRouter(
    prefix="/fonds",
    tags=["Fonds"],
)


@router.get("/get_shares_by_sector")
@cache(expire=30)
async def get_shares_by_sector(
        sector: Sectors,
        limit: int = 10,
        offset: int = 0,
        session: AsyncSession = Depends(get_async_session),
        user: User = Depends(current_user)
):
    query = select(figi_table).where(figi_table.c.sector == sector.name).limit(limit).offset(offset)
    shares = await session.execute(query)
    return shares.mappings().all()


@router.get("/sectors")
@cache(expire=30)
async def get_all_sectors(session: AsyncSession = Depends(get_async_session), user: User = Depends(current_user)):
    query = select(figi_table.c.sector).distinct()
    sectors = await session.execute(query)
    return sectors.mappings().all()


@router.get("/get_fundamentals_by_asset_uid")
@cache(expire=30)
async def get_fundamentals_by_asset_uid(asset_uid: str, session: AsyncSession = Depends(get_async_session), user: User = Depends(current_user)):
    response = await fundamentals(asset_uid)
    return response["fundamentals"]


@router.get("/get_data_by_ticker")
@cache(expire=30)
async def get_data_by_ticker(ticker: str, session: AsyncSession = Depends(get_async_session), user: User = Depends(current_user)):
    query = select(figi_table).where(figi_table.c.ticker == ticker.upper())
    sectors = await session.execute(query)
    return sectors.mappings().all()


@router.get("/get_data_by_name")
@cache(expire=30)
async def get_data_by_name(name: str, session: AsyncSession = Depends(get_async_session), user: User = Depends(current_user)):
    query = select(figi_table).where(func.lower(figi_table.c.name) == name.lower())
    sectors = await session.execute(query)
    return sectors.mappings().all()

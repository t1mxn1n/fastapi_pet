from fastapi import APIRouter, Depends
from fastapi_cache.decorator import cache
from sqlalchemy import select, func, desc
from sqlalchemy.ext.asyncio import AsyncSession

from src.auth.config import current_user
from src.auth.models import User
from src.database import get_async_session
from src.fonds.models import figi as figi_table, Sectors, Fundamental
from src.fonds.models import fundamental as fundamental_table
from src.fonds.utils import fundamentals, get_positions

router = APIRouter(
    prefix="/fonds",
    tags=["Fonds"],
)


@router.get("/get_top_shares_by_sector")
@cache(expire=30)
async def get_top_shares_by_sector(
        sector: Sectors,
        fundamental: Fundamental,
        limit: int = 10,
        offset: int = 0,
        session: AsyncSession = Depends(get_async_session),
        user: User = Depends(current_user)
):
    # todo: add indexes for queries
    # todo: add more filters, buy,sell available and contains 'close'
    match fundamental.name:

        case "pe_ratio_ttm" | "ev_to_ebitda_mrq" | "total_debt_to_equity_mrq":
            query = (select(figi_table, fundamental_table).select_from(figi_table).
                     join(fundamental_table, figi_table.c.asset_uid == fundamental_table.c.asset_uid).
                     where((figi_table.c.sector == sector.name) &
                           (fundamental_table.c[fundamental.name] > 0)).
                     order_by(fundamental_table.c[fundamental.name])).limit(limit).offset(offset)

        case "price_to_sales_ttm" | "price_to_book_ttm":
            query = (select(figi_table, fundamental_table).select_from(figi_table).
                     join(fundamental_table, figi_table.c.asset_uid == fundamental_table.c.asset_uid).
                     where((figi_table.c.sector == sector.name) &
                           (fundamental_table.c[fundamental.name] < 1) &
                           (fundamental_table.c[fundamental.name] > 0)).
                     order_by(fundamental_table.c[fundamental.name])).limit(limit).offset(offset)

        case "roe":
            query = (select(figi_table, fundamental_table).select_from(figi_table).
                     join(fundamental_table, figi_table.c.asset_uid == fundamental_table.c.asset_uid).
                     where((figi_table.c.sector == sector.name) &
                           (fundamental_table.c[fundamental.name] > 0)).
                     order_by(desc(fundamental_table.c[fundamental.name]))).limit(limit).offset(offset)

        case _:
            return {"error": "not found case", "detail": fundamental.name}

    shares = await session.execute(query)

    return shares.mappings().all()


@router.get("/profile_info")
@cache(expire=30)
async def profile_info(api_token: str, session: AsyncSession = Depends(get_async_session), user: User = Depends(current_user)):

    data = await get_positions(api_token)

    return data


@router.get("/sectors")
@cache(expire=30)
async def get_all_sectors(session: AsyncSession = Depends(get_async_session), user: User = Depends(current_user)):
    query = select(figi_table.c.sector).distinct()
    sectors = await session.execute(query)
    return sectors.mappings().all()


@router.get("/get_fundamentals_by_asset_uid")
@cache(expire=30)
async def get_fundamentals_by_asset_uid(asset_uid: str, session: AsyncSession = Depends(get_async_session),
                                        user: User = Depends(current_user)):
    response = await fundamentals([asset_uid])
    return response


@router.get("/get_data_by_ticker")
@cache(expire=30)
async def get_data_by_ticker(ticker: str, session: AsyncSession = Depends(get_async_session),
                             user: User = Depends(current_user)):
    query = select(figi_table).where(figi_table.c.ticker == ticker.upper())
    sectors = await session.execute(query)
    data = sectors.mappings().all()
    if data:
        return data
    return {"detail": "Not Found", "info": f"{ticker}", "method": "get_data_by_ticker"}


@router.get("/get_data_by_name")
@cache(expire=30)
async def get_data_by_name(name: str, session: AsyncSession = Depends(get_async_session),
                           user: User = Depends(current_user)):
    query = select(figi_table).where(func.lower(figi_table.c.name) == name.lower())
    sectors = await session.execute(query)
    data = sectors.mappings().all()
    if data:
        return data
    return {"detail": "Not Found", "info": f"{name}", "method": "get_data_by_name"}

import asyncio
import datetime

from tinkoff.invest import AsyncClient, InstrumentStatus
from pandas import DataFrame
from sqlalchemy import insert, delete, select
from tinkoff.invest.schemas import (
            GetTechAnalysisRequest, IndicatorType, IndicatorInterval, TypeOfPrice, Deviation, Quotation, Smoothing,
InstrumentType, GetAssetFundamentalsRequest
        )

from src.database import scoped_session
from src.config import TINKOFF_API_KEY
from src.fonds.models import figi as figi_table

from pprint import pprint as pp


async def main():
    async with AsyncClient(TINKOFF_API_KEY) as client:
        print(await client.users.get_accounts())


async def figi():
    async with AsyncClient(TINKOFF_API_KEY) as client:
        shares = await client.instruments.shares(instrument_status=InstrumentStatus.INSTRUMENT_STATUS_ALL)
        shares_df = DataFrame(shares.instruments, columns=['name', 'figi', 'ticker', 'class_code'])
        shares_dict = shares_df.to_dict(orient='records')
    async with scoped_session() as session:
        stmt_delete = delete(figi_table)
        await session.execute(stmt_delete)
        stmt_insert = insert(figi_table).values(shares_dict)
        await session.execute(stmt_insert)
        await session.commit()


async def get_ticker_by_figi(figi_value: str):
    async with scoped_session() as s:
        query = select(figi_table).where(figi_table.c.figi == figi_value)
        ticker = await s.execute(query)
        ticker = ticker.mappings().all()
    return ticker


async def get_positions():
    async with AsyncClient(TINKOFF_API_KEY) as client:
        data = await client.users.get_accounts()
        data_broker = list(filter(lambda x: x.name == "Брокерский счёт", data.accounts))
        if len(data_broker) > 1:
            # TODO: do some... :)
            ...
        portfolio = await client.operations.get_portfolio(account_id=data_broker[0].id)
        # data = await client.market_data.get_trading_status(figi="TCSS09805522")
    positions = []
    # await client.instruments.shares()
    for position in portfolio.positions:
        ticker = await get_ticker_by_figi(position.figi)
        pp(position)
        return
        if not ticker:
            continue
        data = {
            "quantity": position.quantity.units,
            "figi": position.figi,
            "current_stock_price": float(f"{position.current_price.units}.{position.current_price.nano}"),
            "ticker": ticker[0]["ticker"],
            "name": ticker[0]["name"],
            "market_class_code": ticker[0]["class_code"],
            "profit_rub": float(f"{position.expected_yield.units}.{position.expected_yield.nano}"),
            "uid": position.instrument_uid
        }
        data["total_price"] = data["current_stock_price"] * data["quantity"]
        positions.append(data)
    return positions


async def info_1():
    """
    Технические индикаторы

    :return:
    """
    async with AsyncClient(TINKOFF_API_KEY) as client:
        # a = await client.instruments.share_by(id_type=1, id='BBG006L8G4H1')

        from datetime import datetime
        share = await client.instruments.share_by(id_type=1, id='BBG006L8G4H1')

        req = GetTechAnalysisRequest(indicator_type=IndicatorType.INDICATOR_TYPE_RSI,
                                     instrument_uid=share.instrument.uid,
                                     from_=datetime.fromtimestamp(1709774073),
                                     to=datetime.now(),
                                     interval=IndicatorInterval.INDICATOR_INTERVAL_10_MIN,
                                     type_of_price=TypeOfPrice.TYPE_OF_PRICE_CLOSE,
                                     length=10,
                                     deviation=Deviation(deviation_multiplier=Quotation(units=10, nano=10)),
                                     smoothing=Smoothing(fast_length=12, slow_length=26, signal_smoothing=9)
                                     )

        a = await client.market_data.get_tech_analysis(request=req)

        pp(a)


async def info():
    """
    фундаментальные показатели
    :return:
    """
    async with AsyncClient(TINKOFF_API_KEY) as client:
        share = await client.instruments.share_by(id_type=1, id='BBG006L8G4H1')
        pp(share.instrument.uid)
        q = await client.instruments.find_instrument(query='BBG006L8G4H1')
        pp(q)
        # a = await client.instruments.get_assets()
        # pp(a.assets)
        a = await client.instruments.get_instrument_by(id_type=1, id='BBG006L8G4H1')
        a = await client.instruments.get_asset_fundamentals(GetAssetFundamentalsRequest(assets=[a.instrument.asset_uid]))
        # a = await client.instruments.get_asset_by(id=a.instrument.asset_uid)
        pp(a)
        # pp(a)


async def test():
    sectors = []
    async with AsyncClient(TINKOFF_API_KEY) as client:
        s = await client.instruments.shares(instrument_status=InstrumentStatus.INSTRUMENT_STATUS_ALL)
    for i in s.instruments:
        if i.sector not in sectors:
            sectors.append(i.sector)
    print(sectors)
    return sectors


if __name__ == "__main__":
    asyncio.run(test())
    # print(asyncio.run(get_positions()))

import asyncio
import datetime
import math

from tinkoff.invest import AsyncClient, InstrumentStatus
from pandas import DataFrame
from sqlalchemy import insert, delete, select, RowMapping, Sequence
from tinkoff.invest.schemas import (
            GetTechAnalysisRequest, IndicatorType,
            IndicatorInterval, TypeOfPrice, Deviation, Quotation,
            Smoothing, GetAssetFundamentalsRequest
        )

from src.database import scoped_session
from src.config import TINKOFF_API_KEY
from src.fonds.models import figi as figi_table

from pprint import pprint as pp

PSQL_QUERY_ALLOWED_MAX_ARGS = 32767


async def figi():

    columns = ['name', 'figi', 'ticker', 'class_code', 'uid', 'sector', 'api_trade_available_flag', 'asset_uid']

    async with AsyncClient(TINKOFF_API_KEY) as client:
        shares = await client.instruments.shares(instrument_status=InstrumentStatus.INSTRUMENT_STATUS_BASE)

    shares_df = DataFrame(shares.instruments, columns=columns)
    # shares_df = shares_df[shares_df["class_code"].isin(["TQBR", "SPBXM"])]
    shares_dict = shares_df.to_dict(orient='records')

    batched_shares_indexes = await batch(len(columns), len(shares_dict))
    await insert_figi_to_db(shares_dict, batched_shares_indexes)


async def batch(args_per_row, total_records):
    """
    PostgreSQL имеет ограничение в 32767 аргументов для единоразовой
    записи в таблицу. Агрументы находятся по формуле:
        args = count_fields_in_row * count_rows
    Данная функция вычисляет границы батчей для записи.
    """
    allowed_args_per_query = int(math.floor(PSQL_QUERY_ALLOWED_MAX_ARGS / args_per_row))

    indexes_args_batches = [
        (x, x + allowed_args_per_query) for x in range(0, total_records, allowed_args_per_query)
    ]
    return indexes_args_batches


async def insert_figi_to_db(shares_dict, batched_indexes):
    async with scoped_session() as session:

        stmt_delete = delete(figi_table)
        await session.execute(stmt_delete)

        for start, end in batched_indexes:
            stmt_insert = insert(figi_table).values(shares_dict[start:end])
            await session.execute(stmt_insert)
        await session.commit()


async def get_ticker_by_figi(figi_value: str):
    async with scoped_session() as s:
        query = select(figi_table).where(figi_table.c.figi == figi_value)
        ticker = await s.execute(query)
        ticker = ticker.mappings().all()
    return ticker


async def get_positions():
    """
    Получить активные позиции по API_KEY
    :return:
    """
    async with AsyncClient(TINKOFF_API_KEY) as client:
        data = await client.users.get_accounts()
        data_broker = list(filter(lambda x: x.name == "Брокерский счёт", data.accounts))
        if len(data_broker) > 1:
            # TODO: do some... :)
            ...
        portfolio = await client.operations.get_portfolio(account_id=data_broker[0].id)
    positions = []
    for position in portfolio.positions:
        ticker = await get_ticker_by_figi(position.figi)

        data = {
            "quantity": position.quantity.units,
            "figi": position.figi,
            "current_stock_price": float(f"{position.current_price.units}.{position.current_price.nano}"),
            "ticker": ticker[0]["ticker"],
            "name": ticker[0]["name"],
            "market_class_code": ticker[0]["class_code"],
            "profit_rub": float(f"{position.expected_yield.units}.{position.expected_yield.nano}"),
            "uid": position.instrument_uid,
            # TODO: asset_uid needed in future
        }
        data["total_price"] = data["current_stock_price"] * data["quantity"]
        positions.append(data)
    return positions


async def technical():
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


async def fundamentals(asset_uid):
    """
    фундаментальные показатели
    :return:
    """
    async with AsyncClient(TINKOFF_API_KEY) as client:
        return await client.instruments.get_asset_fundamentals(GetAssetFundamentalsRequest(assets=[asset_uid]))


async def fundamentals_filter(shares_list: list[RowMapping]):
    for ind, share in enumerate(shares_list):
        data = dict(share)
        fundamentals_share = await fundamentals(share["asset_uid"])
        fundamentals_share_list = fundamentals_share.fundamentals
        if not fundamentals_share_list:
            data |= {"error": "no data"}
            continue
        data["fundamentals"] = fundamentals_share_list[0].__dict__
        shares_list[ind] = data

    # todo: sorting/filtering by fund fields

    return shares_list


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
    asyncio.run(figi())
    # asyncio.run(fundamentals())
    # asyncio.run(test())
    # print(asyncio.run(get_positions()))

import asyncio
import datetime
import math

from grpc import StatusCode
from numpy import nan
from tinkoff.invest import AsyncClient, InstrumentStatus
from pandas import DataFrame
from datetime import datetime, timezone
from tqdm import tqdm
from loguru import logger
from sqlalchemy import insert, delete, select, RowMapping
from sqlalchemy.dialects.postgresql import insert
from tinkoff.invest.schemas import (
    GetTechAnalysisRequest, IndicatorType,
    IndicatorInterval, TypeOfPrice, Deviation, Quotation,
    Smoothing, GetAssetFundamentalsRequest
)
from tinkoff.invest.exceptions import AioRequestError

from src.database import scoped_session, get_async_session
from src.config import TINKOFF_API_KEY
from src.fonds.models import figi as figi_table
from src.fonds.models import fundamental as fundamental_table

from pprint import pprint as pp

PSQL_QUERY_ALLOWED_MAX_ARGS = 32767


async def figi_updater(update=True):
    columns = ['name', 'figi', 'ticker', 'class_code',
               'uid', 'sector', 'api_trade_available_flag',
               'asset_uid', 'exchange', 'buy_available_flag',
               'sell_available_flag']
    async with AsyncClient(TINKOFF_API_KEY) as client:
        shares = await client.instruments.shares(instrument_status=InstrumentStatus.INSTRUMENT_STATUS_BASE)

    shares_df = DataFrame(shares.instruments, columns=columns)

    shares_available = shares_df[(shares_df['buy_available_flag'] == True) &
                                 (shares_df['sell_available_flag'] == True) &
                                 (~shares_df['exchange'].str.contains('close'))]

    shares_available = shares_available.drop(['exchange', 'buy_available_flag', 'sell_available_flag'], axis=1)

    shares_dict = shares_available.to_dict(orient='records')

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


async def fundamentals_updater():
    async with scoped_session() as session:
        query = select(figi_table.c.asset_uid)
        result = await session.execute(query)

        result = result.mappings().all()
        asset_uid_list = list(set([v for share in result for v in share.values()]))
        for asset_uid in tqdm(asset_uid_list):
            fundamentals_data = await fundamentals(asset_uid)
            if not fundamentals_data:
                continue
            data = {
                "asset_uid": asset_uid,
                "pe_ratio_ttm": fundamentals_data[0].pe_ratio_ttm,
                "price_to_sales_ttm": fundamentals_data[0].price_to_sales_ttm,
                "price_to_book_ttm": fundamentals_data[0].price_to_book_ttm,
                "ev_to_ebitda_mrq": fundamentals_data[0].ev_to_ebitda_mrq,
                "roe": fundamentals_data[0].roe,
                "total_debt_to_equity_mrq": fundamentals_data[0].total_debt_to_equity_mrq,
                "update_time": datetime.now().astimezone(timezone.utc)
            }

            await upsert_implementation(session, data)

    # todo: delete old data bu update_time, f.e. with delta=day


async def upsert_implementation(session, data):
    stmt = insert(fundamental_table).values(data)
    stmt = stmt.on_conflict_do_update(
        index_elements=[fundamental_table.c.asset_uid],
        set_=data
    )
    await session.execute(stmt)
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


async def fundamentals(asset_uid, try_number=0):
    """
    фундаментальные показатели
    :return:
    """
    if try_number == 3:
        logger.error("something wrong, 3 tries over")
        return

    async with AsyncClient(TINKOFF_API_KEY) as client:
        try:
            result = await client.instruments.get_asset_fundamentals(GetAssetFundamentalsRequest(assets=[asset_uid]))
            return result.fundamentals

        except AioRequestError as error:

            if error.code == StatusCode.NOT_FOUND:
                return

            if error.code == StatusCode.RESOURCE_EXHAUSTED:
                logger.info(f"available requests exhausted, wait {error.metadata.ratelimit_reset} sec")
                await asyncio.sleep(error.metadata.ratelimit_reset + 0.5)
                try_number += 1
                await fundamentals(asset_uid, try_number)

        except Exception as e:
            logger.error(f"Unrecognized error {e}")
            return


async def fundamentals_filter(shares_list: list[RowMapping]):
    uids = []
    print(len(shares_list))
    for ind, share in enumerate(shares_list):
        uids.append(share["asset_uid"])
    # uids = ['f38554e3-991b-4a7e-8433-b8b22af20e2e', 'f23101b4-d088-40c5-a4fb-f7b57d6d8579', '302fedfd-cba9-4c71-92b8-2141fafb2206', '0294caae-bcfe-4988-8f7d-0c6b0d21da96', '56e205f0-d4b5-41f9-a628-b32cc75761b5', '9893c8b6-451d-4d5c-ab30-00 5e1a08685a', 'd7333221-9f0e-44c7-ae46-68db907df52f', 'c228f2a9-30fe-41a7-91d1-1b3178cc55ce', '2213a2b9-efbb-4882-a0da-b4057095b949', '29b0cb42-0523-4695-88b3-5be935cc79b6', '644919d5-b2d7-43f5-94da-6ddc3f59e89d', '45aeefa1-5da7- 4591-9138-14c60e2207a8', 'cc314b75-fdb8-4357-8fa6-212a28d9f5cc', '09cb3d96-ad1a-42cd-8189-e905c1dee9bd', 'de2695aa-7c6b-4258-a2c2-1cda46a66bb0', '7014e94d-e029-4a66-9764-8aad5ebed2f0', 'bd420453-642c-4659-8ceb-b3c937c633ef', '0d 248d75-e945-4432-b66b-9d52db845f18', 'a6406d6f-a99c-421d-8e02-28ff459ba6ec', 'f6245a09-c759-401e-9ce5-8940f6fd03d7', '5ab96097-e550-4f39-803b-ec4d613c86e9', '42df7b99-1eb5-4f5e-b3a9-389077f7b32b', 'b647e8d7-46de-4f29-bb5a-a1e594 063b2d', 'c0fe5f6a-9f3c-49b4-b55e-05ea216a3591', 'ae6d35f1-d00e-467a-9cd4-ea014bf59e35', 'eeb20996-130c-488b-9b84-0c87ab87d6e8', 'c5a8a9c2-e7ef-46aa-9ae9-299ac9bddf97', '2131a331-8f5c-4d77-98d2-03766b7da76d', '4721cad5-1f0b-4568 -bb76-29bce6c30d09', '2c3eb30c-7769-49fa-918a-6faa1de09fb2', 'b08e7d37-f2cc-4c54-a3a1-a6d704af79da', '5f274cd8-82a2-4e76-9ea1-32c1a0bcb35d', 'a2e2c08b-b477-44a7-a86d-671925eedd81', '95ed74b8-29f0-4f1e-8547-f5eff1669cb3', 'fc2cfd 46-de14-41d6-b40c-70c9d825ffc3', '6474a6b8-91d1-4073-9296-ed63e476ea63', '0111e04b-f72c-48b6-86a3-ae3c8386b9f4', 'ee36e9c2-150a-4b3f-a8d6-8363cdcaae3d', 'f12b5aa9-6a18-4083-a427-8cbd1ae57375', 'ba426f3a-3489-4011-9884-8ac8d332b2 1f', '2abaa5d7-bbbc-4290-b615-deb71ebe5d34', 'a675f47a-30d2-4547-ba73-89fc38161802', '359437db-6282-4afe-a14e-e970ebd46548', '6f4daabf-3fe7-4a61-960e-30dd13fc2bc1', 'fe8d49a4-3321-449b-bd08-02efbac878fe', 'ca30c58b-7fe1-4f62-b86 7-890d1da27f71', 'f26bf959-46e3-4cb8-a931-f42b29af771e', '8da3a687-6e16-4042-9426-2951bf39ba71', 'e27bce44-4411-4e65-9aa7-e2346810af62', 'c8c7634c-b3cf-4a5d-81d8-2a26fd4acc8b', 'd0be351e-8fd3-4246-8b8d-5227b6d02a64', 'b5f712dc-1 c29-4456-b7b0-bc078da0fbe7', '7116c2c1-4ea7-4e62-9388-4bf6953b8421', 'aabc6dd3-1485-4e60-9f75-504b29955dcf', '18db1f5d-44c5-4d81-ad50-24a75f145807', '274bb3f4-3694-4e75-953d-093645b5a8d5', '92ad8b41-a201-4b1b-93cc-dac5fc9c1a6c', '31e26969-afe4-4ae7-ae41-97aeb6700a58', '6adb85f1-45e1-4991-b1c2-effa2844930c', '10f22986-8676-430b-aa06-e447ed166019', 'e841731c-5139-46ad-9a38-74de3286111a', '9d96907e-fa9c-4ff2-9c55-a1308f2713ef', 'a4ad7065-b11b-472d-920d-5a 91a7bd2e4b', '56f63236-214f-4a6b-aeda-c0708a6bdf97', 'c49d95bd-0d22-4199-9366-2df079115ba1', '50885683-e22d-427d-851d-12a9d8bb8042', '6b7385a4-95f4-4bd7-b979-b1cdae283bbf', '7d85059e-8d5d-4095-8d47-caa9aed076d2', 'da3b178d-1c71- 4fbc-81fc-ef6aa9926dbb', '015cea6c-1eb1-4104-98d7-5aece584da7e', 'd6375d52-90e1-434b-90fc-857cf13fe886', 'fff2e4e6-c833-4c1c-b534-76eee85a2690', '41e68af5-54b0-483c-a417-5b85458daeea', '9d9adb56-2235-488f-aead-7f34c696156f', 'ff 1a4ab7-8f50-49b7-91c8-b8b4c46996f6', 'e53c5dfc-11b3-4412-8cc6-b64b1e85929a', '2e38ded5-8510-4592-8fb4-5e48d85dadda', '0ea8e619-c8d2-47c5-8737-9e175dd2c7ae', '19a10303-cd20-48d3-ac01-3daacbf34c37', 'd1dc11b2-2e42-479f-a82f-2d37ce 0942af', '0d9d9375-3ca1-417c-8f77-378af0b581ed', '0d02ede1-cbaa-4da2-9dbd-650beb619999', '48cdfa02-db5c-4297-a853-d3d1e21cf8ae', '77b111a2-2a75-4f92-b130-9fbf8924b8da', '798d25f1-71e8-4105-bd2b-78e47359a2c1', 'c4fca878-fa7d-4db5 -84d1-4c3f8b1fbe27', 'b47831a7-e8ed-44eb-913f-1492a2708386', 'b23bfb81-94c2-4e08-9d81-8dfb37231cdf', '078500bf-38a9-4387-8750-28cd5dbc909e', 'eaf3c6a4-4d05-4698-aac1-ba9bf8ac02f7', '15a2656f-9105-4520-877f-c81e7a8d028b', 'e83b24 95-f461-49d2-9f20-b736a197f655', '2946b587-cc32-419d-9ff0-a7e959c916d5', 'f7ebb244-2fee-48f4-b05c-cf470f98a86a', '8e66a612-ce91-4eda-946b-2c15129f8036', 'bdff2b97-ac83-46da-87f2-7d065a57e7f5', '18a3c96d-827e-40b4-82b6-cb9a2ca42867', '7fe69b86-1095-48e9-91b9-ca7f742cc977', 'e520461a-87f0-4422-b2fe-9b1b577a85a4', 'b59fd4cd-a098-48a9-b4d9-b3180013b96b']
    # uids = ['4203f66a-32e6-46e3-8f44-805aad16d28b', '2c7be540-a8de-4339-89a6-83621f0ea12c', 'f169ac12-2376-4001-9008-abddfbdabb5f', 'fa0d05e7-f8fd-4c67-8030-6961227e6095', 'd2d1e10b-ba5d-4a29-92d2-8572fcceeff9']
    print(len(uids))
    fundamentals_share = await fundamentals(uids)
    print(len(fundamentals_share))
    for i in range(len(shares_list)):
        data = dict(shares_list[i])
        data["fundamentals"] = fundamentals_share[i]
        shares_list[i] = data

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


async def test2():
    async with AsyncClient(TINKOFF_API_KEY) as client:
        s = await client.instruments.share_by(id_type=3, id='aab18c37-e7d1-43e0-a17e-7ba89ae01ecd')
        pp(s.instrument)
        s = await client.instruments.share_by(id_type=3, id='b71bd174-c72c-41b0-a66f-5f9073e0d1f5')
        pp(s.instrument)
        s = await client.instruments.share_by(id_type=3, id='9a88a875-7dde-431a-ad1d-dc8ba3cf8a39')
        pp(s.instrument)


if __name__ == "__main__":
    # asyncio.run(figi_updater())
    asyncio.run(fundamentals_updater())
    # asyncio.run(fundamentals())
    # asyncio.run(test2())
    # print(asyncio.run(get_positions()))

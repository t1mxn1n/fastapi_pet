import asyncio

from tinkoff.invest import AsyncClient, InstrumentStatus
from pandas import DataFrame
from sqlalchemy import insert, delete

from src.database import scoped_session
from src.config import TINKOFF_API_KEY
from src.fonds.models import figi as figi_table


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


if __name__ == "__main__":
    asyncio.run(figi())

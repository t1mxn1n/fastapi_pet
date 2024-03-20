from enum import Enum
from sqlalchemy import MetaData, Table, Column, String, Boolean, Float, DateTime


metadata = MetaData()


figi = Table(
    "figi",
    metadata,
    Column("ticker", String),
    Column("figi", String),
    Column("name", String),
    Column("class_code", String),
    Column("uid", String),
    Column("asset_uid", String),
    Column("sector", String),
    Column("api_trade_available_flag", Boolean)
)


fundamental = Table(
    "fundamental",
    metadata,
    Column("asset_uid", String, primary_key=True),
    Column("pe_ratio_ttm", Float),
    Column("price_to_sales_ttm", Float),
    Column("price_to_book_ttm", Float),
    Column("ev_to_ebitda_mrq", Float),
    Column("roe", Float),
    Column("total_debt_to_equity_mrq", Float),
    Column("update_time", DateTime(timezone=True))
)


class Sectors(Enum):
    consumer = 'consumer'
    electrocars = 'electrocars'
    industrials = 'industrials'
    materials = 'materials'
    financial = 'financial'
    other = 'other'
    it = 'it'
    energy = 'energy'
    health_care = 'health_care'
    telecom = 'telecom'
    real_estate = 'real_estate'
    green_energy = 'green_energy'
    utilities = 'utilities'
    ecomaterials = 'ecomaterials'
    green_buildings = 'green_buildings'


class Fundamental(Enum):
    pe_ratio_ttm = "p/e"
    price_to_sales_ttm = "p/s"
    price_to_book_ttm = "p/b"
    ev_to_ebitda_mrq = "ev/ebitda"
    roe = "roe"
    total_debt_to_equity_mrq = "debt/equity"

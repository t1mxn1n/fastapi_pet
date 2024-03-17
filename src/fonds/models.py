from enum import Enum
from sqlalchemy import MetaData, Table, Column, String, Boolean


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
    ...



from sqlalchemy import MetaData, Table, Column, String, Integer


metadata = MetaData()

figi = Table(
    "figi",
    metadata,
    Column("ticker", String),
    Column("figi", String),
    Column("name", String),
    Column("class_code", String)
)


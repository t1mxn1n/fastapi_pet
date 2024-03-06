from sqlalchemy import MetaData, Table, Column, Integer, String, TIMESTAMP, ForeignKey


metadata = MetaData()

task = Table(
    "task",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("name", String, nullable=False),
    Column("description", String)
)


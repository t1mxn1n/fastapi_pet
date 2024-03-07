from sqlalchemy import MetaData, Table, Column, Integer, String, TIMESTAMP, ForeignKey

from src.auth.models import user

metadata = MetaData()

task = Table(
    "task",
    metadata,
    Column("name", String, nullable=False),
    Column("description", String),
    Column("user_id", Integer, ForeignKey(user.c.id))
)


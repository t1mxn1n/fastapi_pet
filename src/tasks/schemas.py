from pydantic import BaseModel, ConfigDict


class TaskAdd(BaseModel):
    id: int
    name: str
    description: str | None = None


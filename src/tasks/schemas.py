from pydantic import BaseModel


class TaskAdd(BaseModel):
    name: str
    user_id: int
    description: str | None = None

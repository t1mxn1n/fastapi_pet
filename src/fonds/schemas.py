from pydantic import BaseModel


class TaskAdd(BaseModel):
    ticker: str
    figi: str
    name: str
    class_code: str

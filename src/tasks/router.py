from fastapi import APIRouter, Depends
from src.tasks.schemas import TaskAdd
from sqlalchemy import select, insert
from sqlalchemy.ext.asyncio import AsyncSession

from src.database import get_async_session
from src.tasks.models import task as task_table

router = APIRouter(
    prefix="/tasks",
    tags=["Tasks"],
)


@router.post("")
async def add_task(new_task: TaskAdd, session: AsyncSession = Depends(get_async_session)):
    statement = insert(task_table).values(**new_task.dict())
    await session.execute(statement)
    await session.commit()
    return {"status": "success"}


@router.get("")
async def get_tasks(session: AsyncSession = Depends(get_async_session)):
    query = select(task_table)
    tasks = await session.execute(query)
    return tasks.mappings().all()

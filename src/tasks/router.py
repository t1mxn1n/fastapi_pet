import time

from fastapi import APIRouter, Depends

from src.auth.models import User
from src.tasks.schemas import TaskAdd
from sqlalchemy import select, insert
from sqlalchemy.ext.asyncio import AsyncSession
from fastapi_cache.decorator import cache
from celery.result import AsyncResult

from src.database import get_async_session
from src.tasks.models import task as task_table
from src.auth.config import current_user
from src.tasks.tasks import send_email_report, test_celery_my

router = APIRouter(
    prefix="/tasks",
    tags=["Tasks"],
)


@router.post("")
async def add_task(
        new_task: TaskAdd,
        session: AsyncSession = Depends(get_async_session),
        user: User = Depends(current_user)
):
    data = new_task.dict()
    data["user_id"] = user.id
    statement = insert(task_table).values(**data)
    await session.execute(statement)
    await session.commit()
    return {"status": "success"}


@router.get("")
@cache(expire=30)
async def get_tasks(session: AsyncSession = Depends(get_async_session), user: User = Depends(current_user)):
    time.sleep(5)
    query = select(task_table).where(task_table.c.user_id == user.id)
    tasks = await session.execute(query)
    return tasks.mappings().all()


@router.get("/report")
async def get_report(user: User = Depends(current_user)):
    send_email_report.delay(user.id)
    return {"status": 200, "data": "The email has been sent."}


@router.get("/test_celery")
async def test_celery(data: str):
    task = test_celery_my.delay(data)

    return {"status": "10", "task": task.id}


@router.get("/check_task")
async def test_celery(task_id: str):
    res = AsyncResult(task_id)

    return {"task_id": task_id, "state": res.ready(), "result": res.result}


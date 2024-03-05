from fastapi import APIRouter, Depends
from src.tasks.repository import TaskRepository
from src.tasks.schemas import STask, STaskAdd, STaskId

router = APIRouter(
    prefix="/tasks",
    tags=["Таски"],
)


@router.post("")
async def add_task(task: STaskAdd = Depends()) -> STaskId:
    new_task_id = await TaskRepository.add_task(task)
    return {"id": new_task_id}


@router.get("")
async def get_tasks() -> list[STask]:
    tasks = await TaskRepository.get_tasks()
    return tasks

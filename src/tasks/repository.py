from src.database import TaskOrm, new_session
from sqlalchemy import select
from src.tasks.schemas import STaskAdd, STask


class TaskRepository:
    @classmethod
    async def add_task(cls, task: STaskAdd) -> int:
        async with new_session() as session:
            data = task.model_dump()
            new_task = TaskOrm(**data)
            session.add(new_task)
            await session.flush()  # отправляет в базу данных SQL запрос вида INSERT INTO tasks (name, description) VALUES (‘Jack’, NULL) RETURNING id
            await session.commit()
            return new_task.id

    @classmethod
    async def get_tasks(cls) -> list[STask]:
        async with new_session() as session:
            query = select(TaskOrm)
            result = await session.execute(query)  # result - итератор
            task_models = result.scalars().all()
            tasks = [STask.model_validate(task_model) for task_model in task_models]
            return tasks

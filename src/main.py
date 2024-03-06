from fastapi import FastAPI

from src.tasks.router import router as tasks_router


app = FastAPI(title="just a API")
app.include_router(tasks_router)

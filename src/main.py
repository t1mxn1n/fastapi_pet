from fastapi import FastAPI

from src.auth.config import auth_backend, fastapi_users
from src.auth.schemas import UserRead, UserCreate
from src.tasks.router import router as tasks_router


app = FastAPI(title="just a API")
app.include_router(tasks_router)

app.include_router(
    fastapi_users.get_auth_router(auth_backend),
    prefix="/auth/jwt",
    tags=["Auth"],
)

app.include_router(
    fastapi_users.get_register_router(UserRead, UserCreate),
    prefix="/auth",
    tags=["Auth"],
)

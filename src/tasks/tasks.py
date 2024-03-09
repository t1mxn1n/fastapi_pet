import smtplib
import asyncio
from email.message import EmailMessage
from celery import Celery
from sqlalchemy import select

from src.config import SMTP_PASSWORD, SMTP_HOST, SMTP_PORT, SMTP_USER
from src.tasks.models import task as task_table
from src.auth.models import user as user_table
from src.database import scoped_session

celery = Celery('tasks', broker='redis://localhost:6379')
loop = asyncio.get_event_loop()


def get_email_template(username: str, email: str, tasks: list):
    email_template = EmailMessage()
    email_template['Subject'] = 'test'
    email_template['From'] = SMTP_USER
    email_template['To'] = email

    email_template.set_content(
        '<div>'
        f'<h1 style="color: red;">Здравствуйте, {username} 😊</h1>'
        f'Tasks: {str(tasks)}',
        subtype='html'
    )
    return email_template


async def get_tasks(user_id: int):
    async with scoped_session() as s:
        query = select(task_table).where(task_table.c.user_id == user_id)
        tasks = await s.execute(query)
        tasks = tasks.mappings().all()
        query = select(user_table).where(user_table.c.id == user_id)
        user_data = await s.execute(query)
        user_data = user_data.mappings().all()[0]

    email_schema = get_email_template(user_data['username'], user_data['email'], tasks)
    with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT) as server:
        server.login(SMTP_USER, SMTP_PASSWORD)
        server.send_message(email_schema)


@celery.task
def send_email_report(user_id: int):
    loop.run_until_complete(get_tasks(user_id))





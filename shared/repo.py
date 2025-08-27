import asyncpg
from .settings import settings

async def connect():
    return await asyncpg.connect(settings.DATABASE_URL)

async def get_user(conn, tg_user_id: int):
    return await conn.fetchrow("SELECT * FROM users WHERE tg_user_id=$1", tg_user_id)

async def upsert_user_team(conn, tg_user_id: int, full_name: str, team_id: int):
    await conn.execute("""
      INSERT INTO users (tg_user_id, full_name, team_id)
      VALUES ($1,$2,$3)
      ON CONFLICT (tg_user_id) DO UPDATE SET full_name=EXCLUDED.full_name, team_id=EXCLUDED.team_id
    """, tg_user_id, full_name, team_id)

async def iter_team_users(conn, team_id: int):
    return await conn.fetch("SELECT * FROM users WHERE team_id=$1", team_id)

async def log_action(conn, bitrix_task_id: int, tg_user_id: int, action: str, payload: dict):
    await conn.execute("""
      INSERT INTO task_actions (bitrix_task_id,tg_user_id,action,payload)
      VALUES ($1,$2,$3,$4)
    """, bitrix_task_id, tg_user_id, action, payload)

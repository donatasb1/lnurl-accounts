from redis import Redis, ConnectionPool
import psycopg_pool
import logging
from logging.handlers import RotatingFileHandler
import asyncio
from typing import List
import os
import time
from .ln.node import LndRestNode

# bitcoin network
NETWORK = os.getenv("NETWORK")

# PSQL pool
psql_host = os.getenv("POSTGRES_HOST")
psql_port = os.getenv("POSTGRES_PORT")
psql_user = os.getenv("POSTGRES_USER")
psql_pass = os.getenv("POSTGRES_PASSWORD")
psql_coninf = f"postgresql://{psql_user}:{psql_pass}@{psql_host}:{psql_port}/balances"
psql_pool = psycopg_pool.AsyncConnectionPool(conninfo=psql_coninf)

# Redis pool
r_host = os.getenv("REDIS_HOST")
r_port = os.getenv("REDIS_PORT")
r_psw = os.getenv("REDIS_PSW")
redis_pool = ConnectionPool(host=r_host,
                            port=r_port,
                            password=r_psw,
                            db=0, decode_responses=True)

async def get_redis_connection():
    connection = Redis(connection_pool=redis_pool)
    yield connection

node = LndRestNode()

# 
logger = logging.getLogger("app_logger")
logger.setLevel(logging.DEBUG)
log_file = "./app.log"
max_log_size = 1024 * 1024 * 100 # 1 MB
backup_count = 5
handler = RotatingFileHandler(log_file, maxBytes=max_log_size, backupCount=backup_count)
formatter = logging.Formatter('%(asctime)s [%(levelname)s]: %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)


# background tasks
tasks: List[asyncio.Task] = []
def create_task(coro):
    task = asyncio.create_task(coro)
    tasks.append(task)
    return task

def create_permanent_task(func, *args):
    return create_task(catch_everything_and_restart(func, *args))

def cancel_all_tasks():
    for task in tasks:
        try:
            task.cancel()
        except Exception as exc:
            logger.exception("Error while cancelling task")

async def catch_everything_and_restart(func, *args):
    while True:
        try:
            await func(*args)
        except (asyncio.CancelledError, KeyboardInterrupt):
            print('STOPPING background services...')
            raise  # because we must pass this up
        except Exception as exc:
            logger.exception("Background service exception: ")
            time.sleep(10)

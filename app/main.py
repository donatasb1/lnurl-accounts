import os
if not os.getenv("DOCKER_ENV"):
    import dotenv
    dotenv.load_dotenv()
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from .connections import create_permanent_task, cancel_all_tasks, redis_pool, psql_pool, node, logger
from .database import db_init
from .ln.tasks import process_invoice_notifications, process_payment_notifications
from .ln import ln_router
from .btc import btc_router
from .user import user_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    await db_init(psql_pool)
    create_permanent_task(process_invoice_notifications)
    create_permanent_task(process_payment_notifications)
    yield
    redis_pool.close()
    await psql_pool.close()
    cancel_all_tasks()


status = node.status_sync()
print("\nLND Node status: \n", status, "\n")
if status.error_message is None:
    logger.debug({"event": "Node connected", "status": status.__repr__()})
else:
    logger.error({"event": "Node connection error", "error": status.error_message})

app = FastAPI(lifespan=lifespan)
app.include_router(btc_router.router)
app.include_router(ln_router.router)
app.include_router(user_router.router)

# TODO: change auth rules
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],)

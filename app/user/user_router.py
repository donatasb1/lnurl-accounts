from fastapi import APIRouter, HTTPException, status, Depends, Request, Query
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from typing import Annotated
from datetime import timedelta
from ..connections import psql_pool, get_redis_connection
from .auth import decode_access_token, get_password_hash, verify_password, create_access_token, random_k1, ACCESS_TOKEN_EXPIRE_MINUTES
from .base import *
from .crud import BaseCrud


psql = BaseCrud(psql_pool)
security = HTTPBasic()
router = APIRouter()

"""
AUTH 
"""
@router.post("/signup", include_in_schema=False,
          tags=["account"],
          summary="Register user with email")
async def create_user(
    user: User, 
    request: Request
    ) -> SignUpResponse:
    email_exists = await psql.check_email_exists(str(user.email))
    if email_exists > 0:
        raise HTTPException(status_code=400, detail="Email already exists")    
    username_exists = await psql.check_username_exists(user.username)
    if username_exists > 0:
        raise HTTPException(status_code=400, detail="Username already exists")
    userid = random_k1()
    # secondary id
    k1 = random_k1()
    hashed_password = get_password_hash(user.password)
    user_db = UserInDB(
        **user.model_dump(),
        userid=userid,
        k1=k1,
        hashed_password=hashed_password
    )
    await psql.get_unused_address(userid)
    await psql.create_user_entry(user_db)
    return SignUpResponse(
        username=user.username,
        email=user.email,
        userid=userid,
        k1=k1)


# Login
@router.post("/login", include_in_schema=True,
          tags=["account"],
          summary="Get session token")
async def login(
    request: Request, 
    credentials: HTTPBasicCredentials = Depends(security)
) -> LoginResponse:
    """
    Provide username and password in basic HTTP auth header. 
    """
    user = await psql.get_user(credentials.username)
    if not user:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Incorrect username")
    psw = verify_password(credentials.password, user.hashed_password)
    if not psw:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Incorrect password")
    access_token = create_access_token(
        data={"sub": user.userid, "username": user.username},
        expires_delta=timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES))
    # init session to redis
    await init_session(user.userid, access_token)
    return LoginResponse(
        access_token=access_token,
        username=user.username
    )


async def init_session(userid, access_token):
    async for redis_conn in get_redis_connection():
        redis_conn.delete(f"{userid}::session")
        balances = await psql.get_user_balances(userid)
        sess = Session(
            userid=userid,
            token=access_token,
            balances=balances
        )
        redis_conn.hmset(name=f"{userid}::session", mapping=sess.model_dump(), ex=ACCESS_TOKEN_EXPIRE_MINUTES*60)
        return sess

"""
TRANSFERS
"""

@router.delete("/withdraw/request",
            tags=["transfers"],
            summary="Cancel active withdraw request")
async def cancel_withdraw_request(
    k1: Annotated[str, Query(min_length=64, max_length=64)],
    token_data: Annotated[TokenData, Depends(decode_access_token)],
    redis_conn = Depends(get_redis_connection)
) -> WithdrawCancelSuccess:
    if token_data is None:
        raise HTTPException(status_code=400, detail="Invalid token")
    request = await psql.get_withdraw_request(k1)
    if request is None:
        raise HTTPException(status_code=400, detail="Invalid request")    
    if request.status in ["CREATED", "VERIFIED", "QUEUED"]:
        redis_conn.delete(k1)
        await psql.cancel_withdraw_request(k1)
        if request.amount is not None and request.status in ["VERIFIED", "QUEUED"]:
            redis_conn.hincrby(f"{token_data.userid}::session", "balances", request.amount)
    return WithdrawCancelSuccess(k1=k1)


@router.get("/withdraw/request",
         tags=["transfers"],
         summary="Get user withdraw requests")
async def get_withdraw_requests(
    token_data: Annotated[TokenData, Depends(decode_access_token)],
) -> list[WithdrawRequest]:
    if token_data is None:
        raise HTTPException(status_code=400, detail="Invalid token")
    requests = await psql.get_active_withdraw_requests(token_data.userid)
    return requests


@router.get("/transfers",
         tags=["transfers"],
         summary="Get user deposits and withdrawals")
async def get_transfers(
    token_data: Annotated[TokenData, Depends(decode_access_token)],
    limit: int = 100,
    ) -> list[Transfer]:
    if token_data is None:
        raise HTTPException(status_code=400, detail="Invalid token")
    transfers = await psql.get_user_transfers(token_data.userid, min(limit, 500))
    return transfers

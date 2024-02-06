from fastapi import APIRouter, Depends, HTTPException, Query
from redis import Redis
import asyncio
from psycopg import IntegrityError
from typing import Annotated
from datetime import datetime
import json
import os
from ..user.auth import decode_access_token, random_k1, RateLimiter
from ..connections import psql_pool, get_redis_connection, logger, node
from ..user.base import TokenData, WithdrawRequest
from .lnurl import LnurlPayResponse, PayRequestMetadata, LnurlPayActionResponse, MessageAction, encode, LnurlErrorResponse, LnurlSuccessResponse, LnurlWithdrawResponse, CreateLnurlResponse
from .crud import LNCrud


SCHEMA = os.getenv("SCHEMA")
DOMAIN = os.getenv("DOMAIN")
MIN_AVAIL = os.getenv("LN_MIN_AVAIL")
FEE_LIMIT_SAT = os.getenv("FEE_LIMIT_SAT")

MIN_SENDABLE = 1000 * 1000   # 0.45
MAX_SENDABLE = 500000 * 1000 # 5*45.0=220
MIN_SENDABLE = os.getenv("LN_MIN_SENDABLE")
MAX_SENDABLE = os.getenv("LN_MAX_SENDABLE")

psql = LNCrud(psql_pool)
limiter = RateLimiter(60)
router = APIRouter()


@router.get("/withdraw/ln/request",
            tags=["transfers"],
            summary="Create private expiring lnurlw")
async def ln_withdraw_request(
    token_data: Annotated[TokenData, Depends(decode_access_token)],
    redis_conn: Redis = Depends(get_redis_connection)):
    if token_data is None:
        raise HTTPException(status_code=400, detail="Invalid token")        
    # verify balance
    available_r = redis_conn.hget(f"{token_data.userid}::session", "balances")
    if available_r is None:
        raise HTTPException(status_code=400, detail="Invalid token")
    available_db = await psql.get_user_balances(token_data.userid)
    available = min(int(available), available_db)
    if available < MIN_AVAIL:
        raise HTTPException(status_code=400, detail="Insufficient balance")
    pending = await psql.get_user_pending_requests(token_data.userid)
    if pending > 0:
        raise HTTPException(status_code=400, detail="User has pending requests")
    is_limited = await limiter.register(token_data.userid)
    if is_limited:
        raise HTTPException(status_code=400, detail="Please try in a few minutes")
    # create withdraw hash
    random_k1_value = random_k1()
    # create link from hash
    PATH = "/withdraw/ln/cb?k1="
    clearnet_url = SCHEMA+DOMAIN+PATH+random_k1_value
    lnurl_legacy = "lightning:"+encode(clearnet_url)
    lnurlw = "lnurlw://"+DOMAIN+PATH+random_k1_value
    # register request
    req = WithdrawRequest(
        userid=token_data.userid,
        k1=random_k1_value,
        network="LN",
        status="CREATED",
        ts_created=int(datetime.utcnow().timestamp()))
    await psql.create_withdraw_request(req)
    # timer
    redis_conn.set(random_k1_value, value=token_data.userid, ex=600)

    return CreateLnurlResponse(lnurl=lnurl_legacy, lnurlw=lnurlw)


@router.get("/withdraw/ln/cb",
            tags=["transfers"],
            summary="Handle call to generated Lnurlw")
async def lnurlw_callback(
    k1: Annotated[str, Query(min_length=64, max_length=64)],
    redis_conn: Redis = Depends(get_redis_connection)
    ) -> LnurlWithdrawResponse | LnurlErrorResponse:
    # request valid for 10 minutes
    exists = redis_conn.exists(k1)
    if not exists:
        return LnurlErrorResponse(reason="Request expired")
    # get WithdrawRequest from db
    request = await psql.get_withdraw_request(k1)
    if not (request is not None and request.status == "CREATED"):
        return LnurlErrorResponse(reason="Invalid withdraw request")
    request: WithdrawRequest
    balance_r = redis_conn.hget(f"{request.userid}::session", "balances")
    balance_db = await psql.get_user_balances(request.userid)
    balance = min(int(balance_r), balance_db)
    if balance is None:
        return LnurlErrorResponse(reason="Session not found")
    balance = int(json.loads(balance)["balances_avail"]["usd"])
    if balance < MIN_AVAIL:
        return LnurlErrorResponse(reason="Insufficient balance. Min amount: "+ MIN_AVAIL)    
    
    PATH  = "/withdraw"
    callback = SCHEMA + DOMAIN + PATH
    descr = "Some withdraw description"
    await psql.update_withdraw_status(k1=k1, status="VERIFIED")
    return LnurlWithdrawResponse(
        callback=callback,
        k1=k1,
        maxWithdrawable=balance * 1000,
        minWithdrawable=MIN_SENDABLE,
        defaultDescription=descr)


@router.get("/withdraw/ln",
            tags=["transfers"],
            summary="Receive LN invoice associated with Lnurlw")
async def ln_withdraw(
    k1: Annotated[str, Query(max_length=64, min_length=64)],
    pr: str,
    redis_conn: Redis = Depends(get_redis_connection),
    ) -> LnurlSuccessResponse | LnurlErrorResponse:
    userid = redis_conn.get(k1)
    if userid is None:
        await psql.update_withdraw_status(k1=k1, status="EXPIRED", reason="")
        return LnurlErrorResponse(reason="Request expired")
    # call node to decode invoice
    decoded_invoice = await node.decode_invoice(pr)
    if decoded_invoice is None:
        return LnurlErrorResponse(reason="Invoice decode error")
    decoded_invoice.k1 = k1
    balance_r = redis_conn.hget(f"{userid}::session", "balances")
    if balance_r is None:
        await psql.update_withdraw_status(k1=k1, status="REJECTED", reason="No session")
        return LnurlErrorResponse(reason="Authentication error")
    balance_db = await psql.get_user_balances(userid)
    available_balance = min(balance_db, int(balance_r))
    if (decoded_invoice.num_satoshis > available_balance) | (decoded_invoice.num_satoshis < MIN_AVAIL):
        await psql.update_withdraw_status(k1=k1, status="REJECTED", reason="Insufficient balance")
        return LnurlErrorResponse(reason="Insufficient balance")
    try:
        request = await psql.withdraw_redeem_request(k1, decoded_invoice)
    except IntegrityError:
        logger.exception({"error": "Redeem error. Duplicate invoice"})
        await psql.update_withdraw_status(k1=k1, status="REJECTED", reason="Duplicate invoice")
        return LnurlErrorResponse(reason="Invalid request")
    if request is None:
        return LnurlErrorResponse(reason="Invalid request")
    redis_conn.hincrby(userid+"::session", "balances", decoded_invoice.num_satoshis)
    # pay async as per lnurl
    asyncio.create_task(node.pay_invoice(decoded_invoice.bolt11, FEE_LIMIT_SAT))
    return LnurlSuccessResponse()


@router.get("/deposit/ln/request",
            tags=["transfers"],
            summary="Get user deposit lnurlp")
async def create_deposit_request(
    token_data: Annotated[TokenData, Depends(decode_access_token)],
    ):
    if token_data is None:
        raise HTTPException(401, detail="Invalid token")    
    k1 = await psql.get_user_k1(token_data.userid)
    if k1 is None:
        raise HTTPException(401, "User not found")    
    PATH = "/deposit/ln/cb?k1="
    clearnet_url = SCHEMA+DOMAIN+PATH+k1
    lnurl_legacy = "lightning:"+encode(clearnet_url)
    lnurlp = "lnurlp://"+DOMAIN+PATH+k1
    return CreateLnurlResponse(lnurl=lnurl_legacy, lnurlw=lnurlp)


@router.get("/deposit/ln/cb",
            tags=["transfers"],
            summary="Handle call to private Lnurlp"
            )
async def lnurlp_callback(
    k1: Annotated[str, Query(min_length=64, max_length=64)],
    ) -> LnurlPayResponse | LnurlErrorResponse:
    PATH = "/deposit/ln?k1="
    callback = SCHEMA + DOMAIN + PATH + k1
    descr = "Some deposit description"
    return LnurlPayResponse(
        callback=callback,
        minSendable=MIN_SENDABLE,
        maxSendable=MAX_SENDABLE,
        metadata=PayRequestMetadata(text_plain=descr))


@router.get("/deposit/ln",
            tags=["transfers"],
            summary="Lnurlp callback sending invoice"
            )
async def ln_deposit(
    k1: Annotated[str, Query(min_length=64, max_length=64)], 
    amount: Annotated[int, Query(gte=MIN_SENDABLE)],
) -> LnurlPayActionResponse | LnurlErrorResponse:
    
    exists = await psql.get_user_by_k1(k1)
    if not exists:
        return LnurlErrorResponse(reason="Invalid token")
    descr = "Deposit to "
    invoice = await node.create_invoice(amount / 1000, unhashed_description=str.encode(descr))
    if invoice is None:
        return LnurlErrorResponse(reason="Error generating invoice")
    invoice.k1 = k1
    await psql.create_deposit_invoice(invoice)
    # payments processor will see incoming payments
    # and update deposit invoice and transactions
    return LnurlPayActionResponse(
        pr=invoice.bolt11,
        successAction=MessageAction(message="Your funds will be desposited shortly"))

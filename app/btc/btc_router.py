from fastapi import APIRouter, Depends, HTTPException, Query
from redis import Redis
from typing import Annotated
from datetime import datetime
import os
from ..user.base import TokenData, WithdrawRequest
from ..user.auth import decode_access_token, random_k1, RateLimiter
from ..connections import psql_pool, get_redis_connection, logger
from ..bitcoinlib.wallet import CBitcoinAddress, CBitcoinAddressError
from ..bitcoinlib.core import b2x, lx, x, CScript
from .crud import BTCCrud
from .base import DepositNewUtxo, AddressResponse, WithdrawBtcResponse
from .tasks import scan_address


psql = BTCCrud(psql_pool)
limiter = RateLimiter(60)
router = APIRouter()

MIN_AVAIL = os.getenv("BTC_MIN_AVAIL")
SKIP_VERIFICATION = True


@router.get("/deposit/btc/scan",
            tags=["transfers"],
            summary="Scan address for new utxos",
            response_model=list[DepositNewUtxo]
            )
async def deposit_btc_scan(
    address: str,
    token_data: Annotated[TokenData, Depends(decode_access_token)],
    redis_conn: Redis = Depends(get_redis_connection),
):
    """Scan address for new utxos    
    """
    if token_data is None:
        raise HTTPException(status_code=400, detail="Invalid token")
    try:
        pubkey_bytes = CBitcoinAddress(address).to_scriptPubKey()
    except CBitcoinAddressError:
        raise HTTPException(status_code=400, detail="Invalid address")
    # make sure address belongs to user scanning
    exists = await psql.get_address_exists(b2x(pubkey_bytes), token_data.userid)
    if not exists:
        raise HTTPException(status_code=401, detail="Address mismatch")
    scanned_utxos = await scan_address(address)
    if not len(scanned_utxos): 
        return []
    new_utxos = await psql.utxo_verify_new(b2x(pubkey_bytes), scanned_utxos)
    total_amount = sum([n.amount for n in new_utxos])
    redis_conn.hincrby(f"{token_data.userid}::session", "balances", total_amount)
    return new_utxos


@router.get("/deposit/btc/address",
            tags=["transfers"],
            summary="Get unused address for account"
            )
async def create_new_address(
    token_data: Annotated[TokenData, Depends(decode_access_token)],
) -> AddressResponse:
    if token_data is None:
        raise HTTPException(401, "Invalid token")
    user_exists = await psql.get_user_exists(token_data.userid)
    if not user_exists:
        raise HTTPException(401, "User not found")
    pubkey = await psql.get_unused_address(token_data.userid)
    addr = CBitcoinAddress.from_scriptPubKey(CScript(x(pubkey)))
    return AddressResponse(address=str(addr))


@router.get("/withdraw/btc/request",
            tags=["transfers"],
            summary="Provide destination address and amount to make an onchain withdrawal"
            )
async def btc_withdraw_request(
    address: str, 
    requested_amount: Annotated[int, Query(gt=0)],
    token_data: Annotated[TokenData, Depends(decode_access_token)],
    redis_conn: Redis = Depends(get_redis_connection),
) -> WithdrawBtcResponse:
    if token_data is None:
        raise HTTPException(status_code=400, detail="Invalid token")
        # one request per 1minutes for user
    is_limited = await limiter.register(token_data.userid)
    if is_limited:
        raise HTTPException(status_code=400, detail="Please try in a few minutes")    
    try:
        addr = CBitcoinAddress(address)
    except CBitcoinAddressError:
        raise HTTPException(status_code=400, detail="Invalid bitcoin address")
    destination_pubkey = b2x(addr.to_scriptPubKey())
    # verify balance
    available = redis_conn.hget(f"{token_data.userid}::session", "balances")
    if available is None:
        raise HTTPException(status_code=400, detail="Invalid token")
    available = int(available)
    if (available < MIN_AVAIL) or (available < requested_amount):
        raise HTTPException(status_code=400, detail="Insufficient balance")

    pending = await psql.get_user_pending_requests(token_data.userid)
    if pending > 0:
        raise HTTPException(status_code=400, detail="User has pending requests")
    # create withdraw hash
    random_k1_value = random_k1()
    req = WithdrawRequest(
        userid=token_data.userid,
        k1=random_k1_value,
        network="BTC",
        status="CREATED",
        ts_created=int(datetime.utcnow().timestamp()),
        amount=requested_amount,
        destination=destination_pubkey,
    )
    await psql.create_withdraw_request(req)
    if SKIP_VERIFICATION:
        await psql.update_withdraw_status(status="VERIFIED", k1=random_k1_value)
        await psql.withdraw_redeem_request(req)
        redis_conn.hincrby(f"{token_data.userid}::session", "balances", -requested_amount)
    else:
        # send code to email
        # not implemented 
        raise HTTPException(400, "Verification error")
    return WithdrawBtcResponse(k1=req.k1)


@router.get("/withdraw/btc", include_in_schema=False)
async def btc_withdraw(
    k1: Annotated[str, Query(max_length=64, min_length=64)],
    token_data: Annotated[TokenData, Depends(decode_access_token)],
    redis_conn: Redis = Depends(get_redis_connection),
    ) -> WithdrawBtcResponse:
    """Verify withdraw request with k1 token, redeems withdraw_request
    Transfer request amount to locked balances and reduce session balances
    """
    if token_data is None:
        raise HTTPException(status_code=400, detail="Invalid token")
    userid = redis_conn.get(k1)
    if userid is None:
        raise HTTPException(status_code=400, detail="Request expired")
    request = await psql.get_withdraw_request(k1)
    if request is None:
        raise HTTPException(status_code=400, detail="Invalid request token")
    available = redis_conn.hget(f"{token_data.userid}::session", "balances")
    if available is None:
        raise HTTPException(status_code=400, detail="Invalid token")
    available = int(available)
    if (available < MIN_AVAIL) or (available < request.amount):
        await psql.update_withdraw_status(k1=k1, status="REJECTED", reason="Insufficient balance")
        raise HTTPException(status_code=400, detail="Insufficient balance")
    request = await psql.withdraw_redeem_request(request)
    if request is None:
        logger.error({"error": "Attempt to redeem invalid requests", "k1": k1})
        raise HTTPException(400, "Invalid request")
    redis_conn.hincrby(f"{token_data.userid}::session", "balances", -request.amount)        
    return WithdrawBtcResponse(k1=request.k1)

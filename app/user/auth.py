import asyncio
from datetime import timedelta, datetime
from fastapi import Depends, Header
from typing import Optional
from passlib.context import CryptContext
import hashlib
import secrets
import binascii
import jwt
from hdwallet import HDWallet
import os
from ..btc.base import WalletAddressInDb
from ..bitcoinlib.wallet import P2WSHBitcoinAddress
from ..bitcoinlib.core.key import CPubKey
from ..bitcoinlib.core.script import CScript, OP_0, OP_1, OP_3, OP_CHECKMULTISIG
from ..bitcoinlib.core import b2x, x
from ..bitcoinlib import SelectParams
from .base import TokenData


NETWORK = os.getenv("NETWORK")
SECRET_KEY = os.getenv("JWT_SECRET")
ALGORITHM = os.getenv("JWT_ALGO")
ACCESS_TOKEN_EXPIRE_MINUTES = os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES")

SelectParams(NETWORK)

WALLET_MASTER_XPUBKEY = os.getenv("WALLET_MASTER_XPUBKEY")
MASTER_0_SIGNER_PUBKEY = os.getenv("MASTER_0_SIGNER_PUBKEY")
MASTER_1_SIGNER_PUBKEY = os.getenv("MASTER_1_SIGNER_PUBKEY")
master0_cpubkey = CPubKey(x(MASTER_0_SIGNER_PUBKEY))
master1_cpubkey = CPubKey(x(MASTER_1_SIGNER_PUBKEY))

def random_k1():
    random_bytes = secrets.token_bytes(32)  # Generates 32 random bytes
    random_hex = binascii.hexlify(random_bytes).decode()  # Convert bytes to a hexadecimal string
    return random_hex

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

def verify_password(plain_password, hashed_password):
    return pwd_context.verify(plain_password, hashed_password)

def get_password_hash(password):
    return pwd_context.hash(password)

def create_access_token(data: dict, expires_delta: Optional[timedelta] = None):
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(hours=12)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt

def decode_access_token(authorization: str = Header(None)):
    if authorization is None:
        return None
    try:
        token = authorization.split()[1]
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        token_data = TokenData(userid=payload.get("sub"), token=token, username=payload.get("username"))
    except (jwt.ExpiredSignatureError, jwt.InvalidTokenError):
        return None
    return token_data

class RateLimiter:
    """
    Limit requests to 1 per interval seconds
    """
    def __init__(self, interval: int):
        self.interval = interval
        self.request_cache: dict[str, float] = {}
        self.lock = asyncio.Lock()
        asyncio.create_task(self.cleanup())

    async def register(self, key: str) -> bool:
        # return is_limited
        current_time = datetime.utcnow().timestamp()
        async with self.lock:
            last_access_time = self.request_cache.get(key, 0)
            if current_time - last_access_time < self.interval:
                self.request_cache[key] = current_time
                return True
            else:
                self.request_cache[key] = current_time
                return False

    async def cleanup(self):    
        while True:
            await asyncio.sleep(180)
            current_time = datetime.utcnow().timestamp()
            to_rem = []
            for key, last_accessed_time in self.request_cache.items():
                if (last_accessed_time + self.interval) < current_time:
                    to_rem.append(key)
            async with self.lock:
                for k in to_rem:
                    try:
                        self.request_cache.pop(k)
                    except KeyError:
                        continue

def derive_new_address(userid, user_index, change, address_index):
    # root public master key for wallet generation
    master_wallet = HDWallet(symbol="BTC", use_default_path=False).from_xpublic_key(WALLET_MASTER_XPUBKEY)
    path = f"m/{user_index}/{change}/{address_index}"
    user_wallet = master_wallet.from_path(path)
    user_pubkey = user_wallet.public_key()

    # convert pubkey to bytes
    user_pubkey_bytes = bytes.fromhex(user_pubkey)
    seckey = CPubKey(user_pubkey_bytes)

    # creating a multisig p2wsh address
    witness_script = CScript([OP_1, seckey, master0_cpubkey, master1_cpubkey, OP_3, OP_CHECKMULTISIG])
    script_hash = hashlib.sha256(witness_script).digest()
    script_pubkey = CScript([OP_0, script_hash])
    address = P2WSHBitcoinAddress.from_scriptPubKey(script_pubkey)

    address = WalletAddressInDb(
        public_key=user_pubkey,     # underlying user pubkey
        chain_code=user_wallet.chain_code(),
        user_index=user_index,
        userid=userid,
        change=change,
        address_index=address_index,
        path=path,
        witness_script=b2x(witness_script),
        script_pubkey=b2x(script_pubkey),   # script pubkey
        p2wsh=str(address))
    return address

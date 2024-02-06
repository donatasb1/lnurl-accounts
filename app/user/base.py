from pydantic import BaseModel, ConfigDict, EmailStr, Field, validator
from typing import NamedTuple, Optional, Literal
import datetime


class User(BaseModel):
    username: str = Field(pattern="[a-zA-Z0-9]", min_length=3, max_length=30)
    email: EmailStr
    password: str


class UserInDB(BaseModel):
    model_config = ConfigDict(extra='ignore')    
    username: str
    email: str
    userid: str
    k1: str
    hashed_password: str | None = None


class TokenData(BaseModel):
    token: str
    userid: str
    username: str


class LoginResponse(BaseModel):
    access_token: str
    username: str


class SignUpResponse(BaseModel):
    username: str
    userid: str
    email: EmailStr
    k1: str


class WithdrawCancelSuccess(BaseModel):
    k1: str


class WithdrawRequest(BaseModel):
    k1: str
    userid: str
    network: Literal["BTC", "LN"]
    redeemed: bool = False
    status: Literal["CREATED", "VERIFIED", "REJECTED", "EXPIRED", "QUEUED", "IN_FLIGHT", "PAID", "PAYMENT_FAILED"]
    reason: Optional[str] = None
    amount: Optional[int] = None
    destination: Optional[str] = None
    ts_created: Optional[int] = None


class WithdrawTransaction(BaseModel):
    userid: str
    network: Literal["BTC", "LN"]
    txid_hex: str
    amount: int
    fee: int
    ts_created: int


class DepositTransaction(BaseModel):
    userid: str
    network: Literal["BTC", "LN"]
    txid_hex: str
    amount: int
    ts_created: int


class Transfer(BaseModel):
    txtype: Literal["Deposit", "Withdrawal"]
    userid: str
    network: Literal["BTC", "LN"]
    txid_hex: str
    amount: int
    fee: int
    ts_created: int


def datefactory():
    return str(datetime.datetime.utcnow())[:-4]


class OrderUI(BaseModel):
    orderid: Optional[str]
    market: str
    side: Literal['bid', 'ask']
    price: int
    size: int
    expiry: Optional[int] = 0


class OrderSYS(OrderUI):
    # universal order model
    userid: str
    tsid: str = Field(default_factory=datefactory)
    fill: int = 0
    side_name: Optional[str] = None
    market_name: Optional[str] = None
    market_expiry: Optional[str] = None


class Session(BaseModel):
    userid: str
    token: str
    balances: int

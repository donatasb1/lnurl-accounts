from pydantic import BaseModel, validator
from typing import NamedTuple, Optional, Literal
import json


class LNDInvoice(BaseModel):
    """
    Decoded invoice
    """
    k1: Optional[str] = None
    payment_hash: str
    bolt11: Optional[str] = None
    destination: str    
    num_satoshis: int
    timestamp: str
    add_index: str
    expiry: str
    description: str
    description_hash: str
    fallback_addr: str
    cltv_expiry: str
    route_hints: str
    payment_addr: str
    features: str
    preimage: Optional[str] = None
    state: Optional[str] = None
    add_index: Optional[int] = None

    @validator("features", pre=True)
    def convert_dict_to_json(cls, value):
        return json.dumps(value)
    
    @validator("route_hints", pre=True)
    def convert_list_to_json(cls, value):
        return json.dumps(value)


class LNPayment(BaseModel):
    payment_hash: str
    userid: Optional[str] = None
    payment_preimage: str
    value_sat: int
    status: Literal["IN_FLIGHT", "SUCCEEDED", "FAILED", "INITIATED"]
    fee_sat: int
    ts_created: int
    failure_reason: str


class PaymentStatus(NamedTuple):
    payment_hash: str
    payment_preimage: str
    value_sat: int
    status: str
    fee_sat: int


class DepositRequest(BaseModel):
    userid: str
    k1: str
    status: Literal["CREATED", "PAID", "SETTLED", "PAYMENT_FAILED"]
    amount: Optional[int] = None
    ts_created: int


class StatusResponse(NamedTuple):
    error_message: Optional[str]
    balance_msat: int


class InvoiceResponse(NamedTuple):
    # LND invoice create response
    ok: bool
    payment_hash: Optional[str] = None
    payment_request: Optional[str] = None   # bolt11
    error_message: Optional[str] = None


class PaymentResponse(NamedTuple):
    # when ok is None it means we don't know if this succeeded
    ok: Optional[bool] = None
    payment_hash: Optional[str] = None
    fee_msat: Optional[int] = None
    preimage: Optional[str] = None
    error_message: Optional[str] = None


class PaymentStatus(NamedTuple):
    payment_hash: str
    payment_preimage: str
    value_sat: int
    status: str
    fee_sat: int

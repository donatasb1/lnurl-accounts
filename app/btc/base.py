from pydantic import BaseModel
from typing import Optional


class WalletAddressInDb(BaseModel):
    public_key: str
    chain_code: str
    user_index: int
    userid: str
    change: int
    address_index: int
    used: Optional[int] = 0
    path: str
    witness_script: str
    script_pubkey: str
    p2wsh: str


class UtxosInDb(BaseModel):
    public_key: str     # tx recipient pubkey
    txid_hex: str       # txid
    vout: int           # output index in tx
    amount: int         # output amount
    locked: int = 0     # psbt lock
    ts_created: int


class DepositNewUtxo(BaseModel):
    public_key: str     # tx recipient pubkey
    txid_hex: str       # txid
    vout: int           # output index in tx
    amount: int         # output amount    


class AddressResponse(BaseModel):
    address: str


class WithdrawBtcResponse(BaseModel):
    k1: str


class UserWithdrawal(BaseModel):
    k1: str
    userid: str
    public_key: str
    request_amount: int
    remaining_amount: int
    change_amount: int = 0
    fee_rate: float = 1.0
    fee: int = 0
    owned_utxos: list[dict] = []


class WDOut(BaseModel):
    amount: int
    public_key: str
    userid: str
    change: bool
    k1: str = ""


class WDIn(BaseModel):
    txid: str
    vout: int
    amount: int
    public_key: str


class WithdrawalModel(BaseModel):
    requested_amount: int = 0
    remaining_amount: int = 0
    vin_amount: int = 0
    vout_amount: int = 0
    change_amount: int = 0
    fee: int = 0
    fee_rate: int = 0
    fee_covered: int = 0
    user_requests: dict[str, UserWithdrawal] = {}
    vin: list[WDIn] = []
    scripts: list[str] = []
    vout: list[WDOut] = []
    change_vout: list[WDOut] = []
    weight: int = 0
    txid: str = ""
    hash: str = ""
    signed_txhash: str = ""

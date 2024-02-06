"""
LNURL Response Models
"""

from bech32 import bech32_decode, bech32_encode, convertbits
from fastapi.datastructures import URL
import json
import math
from typing import List, Literal, Union, Optional
from pydantic import BaseModel, Field, validator, PositiveInt, HttpUrl


def decode(lnurl: str) -> str:
    hrp, data = bech32_decode(lnurl)
    assert hrp
    assert data
    bech32_data = convertbits(data, 5, 8, False)
    assert bech32_data
    return bytes(bech32_data).decode()


def encode(url: Union[str, URL]) -> str:
    bech32_data = convertbits(str(url).encode(), 8, 5, True)
    assert bech32_data
    lnurl = bech32_encode("lnurl", bech32_data)
    return lnurl.upper()


class CreateLnurlResponse(BaseModel):
    lnurl: str
    lnurlw: str


class LnurlSuccessResponse(BaseModel):
    status: Literal["OK"] = "OK"


class LnurlErrorResponse(BaseModel):
    status: Literal["ERROR"] = "ERROR"
    reason: str

    @property
    def error_msg(self) -> str:
        return self.reason

    @property
    def ok(self) -> bool:
        return False


class PayRequestMetadata(BaseModel):
    text_plain: str
    text_long_desc: Optional[str] = None
    image_png_base64: Optional[str] = None
    image_jpeg_base64: Optional[str] = None

    def create_json(self):
        return json.dumps([[k, v] for k, v in self.model_dump().items() if v is not None])
    

class LnurlPayResponse(BaseModel):
    tag: Literal["payRequest"] = "payRequest"
    callback: HttpUrl
    min_sendable: PositiveInt = Field(..., alias="minSendable")
    max_sendable: PositiveInt = Field(..., alias="maxSendable")
    metadata: PayRequestMetadata

    @validator("max_sendable")
    def max_less_than_min(cls, value, values, **kwargs):  # noqa
        if "min_sendable" in values and value < values["min_sendable"]:
            raise ValueError("`max_sendable` cannot be less than `min_sendable`.")
        return value

    @property
    def min_sats(self) -> int:
        return int(math.ceil(self.min_sendable / 1000))

    @property
    def max_sats(self) -> int:
        return int(math.floor(self.max_sendable / 1000))


class MessageAction(BaseModel):
    tag: Literal["message"] = "message"
    message: str = Field(min_length=1, max_length=144)


class UrlAction(BaseModel):
    tag: Literal["url"] = "url"
    url: HttpUrl
    description: str


class LnurlPayRouteHop(BaseModel):
    node_id: str = Field(..., alias="nodeId")
    channel_update: str = Field(..., alias="channelUpdate")


class LnurlPayActionResponse(BaseModel):
    pr: str
    success_action: Optional[Union[MessageAction, UrlAction]] = Field(None, alias="successAction")
    routes: List[List[LnurlPayRouteHop]] = []
    

class LnurlErrorResponse(BaseModel):
    status: Literal["ERROR"] = "ERROR"
    reason: str

    @property
    def error_msg(self) -> str:
        return self.reason

    @property
    def ok(self) -> bool:
        return False


class LnurlSuccessResponse(BaseModel):
    status: Literal["OK"] = "OK"

class LnurlWithdrawResponse(BaseModel):
    tag: Literal["withdrawRequest"] = "withdrawRequest"
    callback: HttpUrl
    k1: str
    min_withdrawable: PositiveInt = Field(..., alias="minWithdrawable")
    max_withdrawable: PositiveInt = Field(..., alias="maxWithdrawable")
    default_description: str = Field("", alias="defaultDescription")

    @validator("max_withdrawable")
    def max_less_than_min(cls, value, values, **kwargs):  # noqa
        if "min_withdrawable" in values and value < values["min_withdrawable"]:
            raise ValueError("`max_withdrawable` cannot be less than `min_withdrawable`.")
        return value
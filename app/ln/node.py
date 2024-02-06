"""
LND node REST interface
"""

from .base import (
    PaymentResponse,
    PaymentStatus,
    StatusResponse,
    LNDInvoice,
    LNPayment
)
from typing import Optional
import codecs
import asyncio
import base64
import hashlib
import json
from typing import AsyncGenerator, Dict, Optional
import httpx
from typing import Optional
import os


MACAROON_PATH = os.getenv("MACAROON_PATH")
CERT_PATH = os.getenv("CERT_PATH")
LND_HOST = os.getenv("LND_HOST")


def fee_reserve(amount_msat: int) -> int:
    reserve_min = 30000
    reserve_percent = 5
    return max(int(reserve_min), int(amount_msat * reserve_percent / 100.0))


class LndRestNode:
    """https://api.lightning.community/rest/index.html#lnd-rest-api-reference"""

    def __init__(self):
        endpoint = LND_HOST
        cert = CERT_PATH
        macaroon = self.load_macaroon()

        if not endpoint:
            raise Exception("cannot initialize lndrest: no endpoint")

        if not macaroon:
            raise Exception("cannot initialize lndrest: no macaroon")

        if not cert:
            raise Exception("cannot initialize lndrest: no certificate")

        endpoint = endpoint[:-1] if endpoint.endswith("/") else endpoint
        self.endpoint = endpoint
        self.macaroon = macaroon

        self.cert = cert or True
        self.auth = {"Grpc-Metadata-macaroon": self.macaroon}
        self.client = httpx.AsyncClient(
            base_url=self.endpoint, headers=self.auth, verify=self.cert
        )
        self.sync_client = httpx.Client(
            base_url=self.endpoint, headers=self.auth, verify=self.cert            
        )
        

    def load_macaroon(self) -> bytes:
        macaroon = codecs.encode(open(MACAROON_PATH, 'rb').read(), 'hex')
        return macaroon

    async def cleanup(self):
        try:
            await self.client.aclose()
        except RuntimeError as e:
            pass

    def status_sync(self):
        try:
            r = self.sync_client.get("/v1/balance/channels", timeout=5)
            r.raise_for_status()
        except (httpx.ConnectError, httpx.RequestError) as exc:
            return StatusResponse(f"Unable to connect to {self.endpoint}. {exc}", 0)
        try:
            data = r.json()
            if r.is_error:
                raise Exception
        except Exception:
            return StatusResponse(r.text[:200], 0)
        return StatusResponse(None, int(data["balance"]) * 1000)        

    async def status(self) -> StatusResponse:
        """
        Get channel balance
        """
        try:
            r = await self.client.get("/v1/balance/channels", timeout=5)
            r.raise_for_status()
        except (httpx.ConnectError, httpx.RequestError) as exc:
            return StatusResponse(f"Unable to connect to {self.endpoint}. {exc}", 0)

        try:
            data = r.json()
            if r.is_error:
                raise Exception
        except Exception:
            return StatusResponse(r.text[:200], 0)
        return StatusResponse(None, int(data["balance"]) * 1000)

    async def create_invoice(
        self,
        amount: int,
        memo: Optional[str] = None,
        description_hash: Optional[bytes] = None,
        unhashed_description: Optional[bytes] = None,
        **kwargs,
    ) -> LNDInvoice | None:
        """
        """
        data: Dict = {"value": amount, "private": True, "memo": memo or ""}
        if kwargs.get("expiry"):
            data["expiry"] = kwargs["expiry"]
        if description_hash:
            data["description_hash"] = base64.b64encode(description_hash).decode(
                "ascii"
            )
        elif unhashed_description:
            data["description_hash"] = base64.b64encode(
                hashlib.sha256(unhashed_description).digest()
            ).decode("ascii")

        r = await self.client.post(url="/v1/invoices", json=data)
        if r.is_error:
            return None
        data = r.json()
        decoded_invoice = await self.decode_invoice(data["payment_request"])
        decoded_invoice.add_index = data["add_index"]
        return decoded_invoice

    async def pay_invoice(self, bolt11: str, fee_limit_msat: int) -> PaymentResponse:
        # set the fee limit for the payment
        lnrpcFeeLimit = dict()
        lnrpcFeeLimit["fixed"] = f"{fee_limit_msat}"
        r = await self.client.post(
            url="/v1/channels/transactions",
            json={"payment_request": bolt11, "fee_limit": lnrpcFeeLimit},
            timeout=20,
        )
        if r.is_error or r.json().get("payment_error"):
            error_message = r.json().get("payment_error") or r.text
            print("Invoice pay error ", error_message)
            return PaymentResponse(False, None, None, None, error_message)
        data = r.json()
        payment_hash = base64.b64decode(data["payment_hash"]).hex()
        fee_msat = int(data["payment_route"]["total_fees_msat"])
        preimage = base64.b64decode(data["payment_preimage"]).hex()
        return PaymentResponse(True, payment_hash, fee_msat, preimage, None)

    async def get_invoice_status(self, payment_hash: str) -> PaymentStatus:
        r = await self.client.get(url=f"/v1/invoice/{payment_hash}")

        if r.is_error or not r.json().get("settled"):
            # this must also work when payment_hash is not a hex recognizable by lnd
            # it will return an error and no "settled" attribute on the object
            return PaymentStatus(None)
        return PaymentStatus(True)

    async def get_payment_status(self, payment_hash: str) -> PaymentStatus:
        """
        This routine checks the payment status using routerpc.TrackPaymentV2.
        """
        # convert checking_id from hex to base64 and some LND magic
        try:
            payment_hash = base64.urlsafe_b64encode(bytes.fromhex(payment_hash)).decode(
                "ascii"
            )
        except ValueError:
            return PaymentStatus(None)
        url = f"/v2/router/track/{payment_hash}"
        # check payment.status:
        # https://api.lightning.community/?python=#paymentpaymentstatus
        statuses = {
            "UNKNOWN": None,
            "IN_FLIGHT": None,
            "SUCCEEDED": True,
            "FAILED": False,
        }
        async with self.client.stream("GET", url, timeout=None) as r:
            async for json_line in r.aiter_lines():
                try:
                    line = json.loads(json_line)
                    if line.get("error"):
                        return PaymentStatus(None)
                    payment = line.get("result")
                    if payment is not None and payment.get("status"):
                        return PaymentStatus(
                            paid=statuses[payment["status"]],
                            fee_msat=payment.get("fee_msat"),
                            preimage=payment.get("payment_preimage"),
                        )
                    else:
                        return PaymentStatus(None)
                except Exception:
                    continue
        return PaymentStatus(None)

    async def list_payments(self, latest_ts: str) -> list[LNPayment]:
        url = "/v1/payments?creation_date_start="+latest_ts
        r = await self.client.get(url)
        if r.is_error:
            error_message = r.text
            return None
        try:
            response = r.json()
            all_payments = []
            for data in response["payments"]:
                payment = LNPayment(
                    payment_hash=data["payment_hash"],
                    payment_preimage=data["payment_preimage"],
                    value_sat=data["value_sat"],
                    status=data["status"],
                    fee_sat=data["fee_sat"],
                    ts_created=int(data["creation_time_ns"] / 1000000000),
                    failure_reason=data["failure_reason"]
                )
                all_payments.append(payment)
            return all_payments
        except Exception as e:
            return None


    async def paid_invoices_stream(self, idx: int = 0) -> AsyncGenerator[LNDInvoice, None]:
        while True:
            try:
                url = "/v1/invoices/subscribe?add_index="+str(idx)
                async with self.client.stream("GET", url, timeout=None) as r:
                    async for line in r.aiter_lines():
                        try:
                            data = json.loads(line)["result"]
                        except Exception:
                            continue
                        payment_hash = base64.b64decode(data["r_hash"]).hex()
                        data["decoded_hash"] = payment_hash
                        invoice = LNDInvoice(
                            payment_hash=payment_hash,
                            bolt11=data["payment_request"],
                            preimage=data["r_preimage"],
                            state=data["state"],
                            destination=data["payment_addr"],
                            num_satoshis=data["value"],
                            timestamp=data["creation_date"],
                            expiry=data["expiry"],
                            description=data["memo"],
                            description_hash=data["description_hash"],
                            fallback_addr=data["fallback_addr"],
                            cltv_expiry=data["cltv_expiry"],
                            route_hints=data["route_hints"],
                            payment_addr=data["payment_addr"],
                            features=data["features"],
                            add_index=data["add_index"],
                        )
                        yield invoice
            except Exception as exc:
                await asyncio.sleep(5)

    async def invoices_stream(self) -> AsyncGenerator[str, None]:
        while True:
            try:
                url = "/v1/invoices/subscribe"
                async with self.client.stream("GET", url, timeout=None) as r:
                    async for line in r.aiter_lines():
                        try:
                            inv = json.loads(line)["result"]
                        except Exception:
                            continue
                        payment_hash = base64.b64decode(inv["r_hash"]).hex()
                        inv["decoded_hash"] = payment_hash
                        yield inv
            except Exception as exc:
                await asyncio.sleep(5)

    async def decode_invoice(self, pay_req: str) -> LNDInvoice | None:
        url = "v1/payreq/"+pay_req
        r = await self.client.get(url)
        if r.is_error:
            error_message = r.json().get("payment_error") or r.text
            return None
        try:
            data = r.json()
            invoice = LNDInvoice(**data)
            invoice.bolt11 = pay_req
            invoice.state = "ACCEPTED"
        except Exception as e:
            return None
        return invoice

    async def get_peer_ids(self) -> list[str]:
        response = await self.client.get("/v1/peers")
        if response.status_code == 200:
            return [p["pub_key"] for p in response.json()["peers"]]
        else:
            return None

    async def track_payments(self) -> AsyncGenerator[PaymentStatus, None]:
        url = "/v2/router/payments"
        async with self.client.stream("GET", url, timeout=None) as r:
            async for json_line in r.aiter_lines():
                try:
                    line = json.loads(json_line)
                    if line.get("error"):
                        continue
                    payment = line.get("result")
                    if payment is not None and payment.get("status", False):
                        yield PaymentStatus(
                            payment_hash=payment["payment_hash"],
                            payment_preimage=payment.get("payment_preimage"),
                            value_sat=payment.get("value_sat"),
                            status=payment["status"],
                            fee_sat=payment.get("fee_sat"),
                        )
                except Exception as e:
                    continue
    
    async def scan_invoices(self, idx=0) -> list[LNDInvoice]:
        url = "/v1/invoices?index_offset="+str(idx)
        response = await self.client.get(url)
        all_payments = []
        if response.status_code == 200:
            for data in response.json().get("invoices", []):
                payment_hash = base64.b64decode(data["r_hash"]).hex()
                data["decoded_hash"] = payment_hash
                invoice = LNDInvoice(
                    payment_hash=payment_hash,
                    bolt11=data["payment_request"],
                    preimage=data["r_preimage"],
                    state=data["state"],
                    destination=data["payment_addr"],
                    num_satoshis=data["value"],
                    timestamp=data["creation_date"],
                    expiry=data["expiry"],
                    description=data["memo"],
                    description_hash=data["description_hash"],
                    fallback_addr=data["fallback_addr"],
                    cltv_expiry=data["cltv_expiry"],
                    route_hints=data["route_hints"],
                    payment_addr=data["payment_addr"],
                    features=data["features"],
                    add_index=data["add_index"],
                )
                all_payments.append(invoice)
        return all_payments
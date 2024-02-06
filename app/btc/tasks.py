from ..bitcoinlib.rpc import Proxy
from ..bitcoinlib.core import lx, b2x, COIN
from .crud import BTCCrud
from .base import WithdrawalModel
from psycopg import IntegrityError
import httpx
import asyncio
from ..connections import NETWORK, logger


async def get_tx_status(WD: WithdrawalModel, psql: BTCCrud):
    url = "https://blockstream.info/testnet/api/tx/"+WD.txid+"/status"
    async with httpx.AsyncClient() as client:
        while True:
            r = await client.get(url)
            if r.status_code != 200:
                logger.error({"error": "TX status response error", "message": r.text})
                await asyncio.sleep(10)
                continue
            status = r.json()
            if status.get("confirmed", False):
                try:
                    await psql.create_withdraw_transaction(2, WD)
                except IntegrityError as e:
                    logger.exception("Error finalizing BTC withdraw transaction in SQL")
                finally:
                    return
            await asyncio.sleep(30)

async def track_withdraw_tx(WD: WithdrawalModel, psql: BTCCrud):
    # url = "https://blockstream.info/api/tx/"+WD.txid+"/status"
    node = Proxy(btc_conf_file='app/bitcoin.conf', timeout=60)
    print('start tracking tx')
    while True:
        try:
            n_confirmations = node.getconfirmations(b2x(lx(WD.txid)), 0)
            print('N confirmations', n_confirmations)
            if n_confirmations > 2:
                await psql.create_withdraw_transaction(n_confirmations, WD)
                return
        except IndexError:
            print('Transaction not yet included', WD.txid)
        await asyncio.sleep(10)
    

async def scan_address(address: str) -> list[dict]:
    async with httpx.AsyncClient() as client:
        # url = "https://blockchain.info/unspent?active="+address+"&confirmations=3"
        if NETWORK == "mainnet":
            url = "https://blockstream.info/api/address/"+address+"/utxo"
        elif NETWORK == "testnet":
            url = "https://blockstream.info/"+NETWORK+"/api/address/"+address+"/utxo"
            r = await client.get(url)
            if r.status_code != 200:
                print("Scan address status", r.status_code)
                return []
            unspents = []
            print('JSON response', r.json())
            for u in r.json():
                # big endian
                txid_hex = u["txid"]
                vout = int(u["vout"])
                amount = int(u["value"])
                unspents.append((txid_hex, vout, amount))
            return unspents                        
            
        else:
            # scantxoutses
            # r = await client.get(url)
            node = Proxy(btc_conf_file='app/bitcoin.conf', timeout=60)
            result = node.call("scantxoutset", "start", [f"addr({address})"])
            unspents = []
            for u in result.get("unspents", []):
                # big endian
                txid_hex = b2x(lx(u["txid"]))
                vout = int(u["vout"])
                amount = float(u["amount"])*COIN
                unspents.append((txid_hex, vout, int(amount)))
            return unspents



# blockstream
"""
[{'txid': '1373fb217fe8ab181c8235aefdd373e77a496d8a21397db01df37c0f65180f27',
  'vout': 2,
  'status': {'confirmed': True,
   'block_height': 824649,
   'block_hash': '000000000000000000015c132538d853642c92271b593c8664b9d7d06b1fe34c',
   'block_time': 1704564866},
  'value': 11147500}]
"""

# blockchain.info
"""
{
"unspent_outputs": [
    {
    "tx_age": "1322659106",
    "tx_hash": "e6452a2cb71aa864aaa959e647e7a4726a22e640560f199f79b56b5502114c37",
    "tx_index": "12790219",
    "tx_output_n": "0",
    "script": "76a914641ad5051edd97029a003fe9efb29359fcee409d88ac",
    "value": "5000661330"
    }
]
}
"""
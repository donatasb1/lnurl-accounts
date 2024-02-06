from ..user.base import WithdrawRequest
from ..user.auth import derive_new_address
from ..user.crud import PSQLClient
from .base import WalletAddressInDb, UtxosInDb, WithdrawalModel
from datetime import datetime
import psycopg_pool
import psycopg
from psycopg.rows import dict_row


class BTCCrud(PSQLClient):

    def __init__(self, pool: psycopg_pool.AsyncConnectionPool):
        super().__init__(pool)

    """
    ADDRESS
    """
    async def create_wallet_address(self, address: WalletAddressInDb):
        q = """
        INSERT INTO wallet_addresses
        (
            public_key, chain_code, user_index, userid, change, 
            address_index, path, witness_script, script_pubkey, p2wsh
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING
        """
        return await self.execute(q, *address.model_dump(exclude=['used']).values())

    async def create_next_address(self, userid: str, change = 0) -> str:
        next_idx = await self.get_next_addressindex(userid, change)
        user_index = next_idx['user_index']
        address_index = next_idx['next_index']
        address = derive_new_address(userid, user_index, change, address_index)
        await self.create_wallet_address(address)
        return address.script_pubkey

    async def get_user_exists(self, userid: str) -> int:
        q = """
        SELECT COUNT(userid) exists
        FROM users
        WHERE userid = %s
        """
        exists = await self.fetchone(q, userid)
        return exists.get("exists", 0)

    async def get_unused_address(self, userid: str) -> str:
        """Get unused script public key"""
        q = """
        SELECT script_pubkey
        FROM wallet_addresses
        WHERE userid = %s
        AND used = 0
        """
        addr =  await self.fetchone(q, userid)
        if addr is None:
            return await self.create_next_address(userid)
        return addr['script_pubkey']

    async def get_unused_address_username(self, username: str) -> str:
        """Get unused script public key"""
        q = """
        SELECT userid
        FROM users
        WHERE username = %s
        """
        q2 = """
        SELECT script_pubkey
        FROM wallet_addresses
        WHERE userid = %s
        AND used = 0
        """        
        userid =  await self.fetchone(q, username)
        addr =  await self.fetchone(q2, userid.get("userid", 0))
        if addr is None:
            return await self.create_next_address(username)
        return addr['script_pubkey']

    async def get_max_userindex(self) -> int:
        q = """
        SELECT MAX(user_index) ci
        FROM wallet_addresses
        """
        result = await self.fetchone(q)
        if result['ci'] is None:
            return 1000
        return result['ci']

    async def get_next_addressindex(self, userid: str, change: int):
        q = """
        SELECT MAX(address_index) + 1 next_index, user_index
        FROM wallet_addresses
        WHERE userid = %s
        AND change = %s
        GROUP BY user_index
        """
        idx = await self.fetchone(q, userid, change)
        if idx is None:
            max_idx = await self.get_max_userindex()
            return {'next_index': 0, 'user_index': max_idx+1}
        return idx

    async def get_address_exists(self, public_key: str, userid: str) -> int:
        q = """
        SELECT COUNT(*) exists
        FROM wallet_addresses
        WHERE script_pubkey = %s
        AND userid = %s
        """
        exists = await self.fetchone(q, public_key, userid)
        return exists.get("exists", 0)
    
    """
    WITHDRAW
    """

    async def create_withdraw_request(self, request: WithdrawRequest):
        q = """
        INSERT INTO withdraw_requests
        (
            userid, k1, network, status, amount, destination, ts_created
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        return await self.execute(q, request.userid, request.k1, request.network, 
                                  request.status, request.amount,
                                  request.destination, request.ts_created)

    async def get_queued_requests(self) -> list[WithdrawRequest]:
        q = """
        SELECT wr.*, fr.rate
        FROM withdraw_requests AS wr
        JOIN feerates AS fr
        ON wr.userid = fr.userid
        WHERE wr.status = 'QUEUED'
        AND wr.network = 'BTC'
        AND fr.network = 'BTC'
        AND redeemed = TRUE
        ORDER BY wr.ts_created ASC
        LIMIT 20
        """
        return await self.fetchmany(q)

    async def get_withdraw_request(self, k1: str) -> WithdrawRequest | None:
        q = """
        SELECT *
        FROM withdraw_requests
        WHERE k1 = %s
        """
        request = await self.fetchone(q, k1)
        if not request:
            return None
        return WithdrawRequest(**request)

    async def get_user_pending_requests(self, userid: str) -> int:
        q = """
        UPDATE withdraw_requests
        SET status = 'EXPIRED'
        WHERE ts_created < %s
        AND status = 'CREATED'
        """
        q2 = """
        SELECT COUNT(k1) as pending
        FROM withdraw_requests
        WHERE userid = %s
        AND status NOT IN ('PAID', 'SETTLED', 'REJECTED', 'PAYMENT_FAILED', 'EXPIRED')
        AND redeemed = FALSE
        """
        ts = int(datetime.utcnow().timestamp() - 60*5)
        await self.execute(q, ts-90)
        count_requests = await self.fetchone(q2, userid)
        return count_requests.get("pending", 0)

    async def update_withdraw_status(self, status: str, k1: str = None, reason: str = "") -> None:
        q = """
        UPDATE withdraw_requests
        SET status = %s,
        reason = %s
        WHERE k1 = %s
        """
        return await self.execute(q, status, reason, k1) 

    async def withdraw_redeem_request(self, request: WithdrawRequest):
        q1 = """
        SELECT *
        FROM withdraw_requests
        WHERE k1 = %s
        AND status = 'VERIFIED'
        AND redeemed = FALSE
        """
        q2 = """
        UPDATE balances
        SET amount = amount - %s
        WHERE userid = %s
        """
        q3 = """
        INSERT INTO locked_balances(userid, k1, amount)
        VALUES (%s, %s, %s)
        """
        q4 = """
        UPDATE withdraw_requests
        SET redeemed = TRUE,
        STATUS = 'QUEUED'
        WHERE k1 = %s
        AND status = 'VERIFIED'
        """
        current_time = int(datetime.utcnow().timestamp())
        async with self.pool.connection() as conn:
            await conn.set_autocommit(True)
            async with conn.cursor(row_factory=dict_row) as cur:
                cur: psycopg.Cursor
                await cur.execute(q1, (request.k1, ))
                request = await cur.fetchone()
                if request is None:
                    return None
                async with conn.transaction():
                    await cur.execute(q2, (request['amount'], request['userid'], ))
                    await cur.execute(q3, (request['userid'], request['k1'], request['amount']))
                    await cur.execute(q4, (request['k1'], ))
        return WithdrawRequest(**request)

    async def get_address_utxos(self, public_key: str) -> list[UtxosInDb]:
        q = """
        SELECT utxos.*
        FROM utxos
        JOIN wallet_addresses AS wa
        ON utxos.public_key = wa.script_pubkey
        WHERE wa.p2wsh = %s
        """
        utxos = await self.fetchmany(q, public_key)
        return [UtxosInDb(**u) for u in utxos]
    
    async def utxo_verify_new(self, script_pubkey: str, scanned_utxos: list[dict]) -> list[UtxosInDb]:
        db_utxos = await self.get_address_utxos(script_pubkey)
        db_utxos_red =  [(u.txid_hex, u.vout) for u in db_utxos]
        unseen = []
        current_time = int(datetime.utcnow().timestamp())
        for u in scanned_utxos:
            if (u[0], u[1]) in db_utxos_red: continue
            utxo = UtxosInDb(
                public_key=script_pubkey,
                txid_hex=u[0],
                vout = u[1],
                amount = u[2],
                locked=0,
                ts_created=current_time,)
            try:
                await self.create_deposit_utxo(utxo)
                unseen.append(utxo)
            except psycopg.errors.IntegrityError:
                continue
        return unseen


    async def create_deposit_utxo(self, utxo: UtxosInDb) -> None:
        """
        utxos added when received from provider / node
        """
        q = """
        INSERT INTO utxos
        (
            userid, public_key, txid_hex, vout, amount, locked, ts_created
        )
        SELECT wa.userid, %s, %s, %s, %s, %s, %s
        FROM wallet_addresses AS wa
        WHERE wa.script_pubkey = %s
        """
        q2 = """
        UPDATE balances AS b
        SET amount = b.amount + %s
        FROM wallet_addresses AS wa
        WHERE b.userid = wa.userid
        AND wa.script_pubkey = %s
        """
        q3 = """
        UPDATE wallet_addresses
        SET used = used + 1
        WHERE script_pubkey = %s
        """
        q4 = """
        INSERT INTO deposit_transactions
        (userid, network, txid_hex, vout, amount, ts_created)
        SELECT wa.userid, 'BTC', %s, %s, %s, %s
        FROM wallet_addresses AS wa
        WHERE script_pubkey = %s
        """
        current_time = int(datetime.utcnow().timestamp())
        async with self.pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                async with conn.transaction():
                    await cur.execute(q, list(utxo.model_dump().values()) + [utxo.public_key])
                    await cur.execute(q2, (utxo.amount, utxo.public_key, ))
                    await cur.execute(q3, (utxo.public_key, ))
                    await cur.execute(q4, (utxo.txid_hex, utxo.vout, utxo.amount, current_time, utxo.public_key, ))
                return

    async def finalize_payment(self, WD: WithdrawalModel):
        q = """
        INSERT INTO btc_payments
        (txid_hex, amount, fee, fee_covered, confirmations)
        VALUES (%s, %s, %s, %s, %s)
        """
        q2 = """
        INSERT INTO change_outs
        (txid_hex, vout, amount, userid, public_key)
        VALUES (%s, %s, %s, %s, %s)
        """
        q3 = """
        INSERT INTO wd_outs
        (k1, txid_hex, vout, amount, public_key)
        VALUES (%s, %s, %s, %s, %s)     
        """
        q4 = """
        INSERT INTO wd_ins
        (txid_hex, txid_hex_prev, vout, amount, public_key)
        VALUES (%s, %s, %s, %s, %s)
        """
        q5 = """
        UPDATE withdraw_requests
        SET status = 'IN-FLIGHT'
        WHERE k1 = %s
        """
        current_time = int(datetime.utcnow().timestamp())
        async with self.pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                async with conn.transaction():
                    await cur.execute(q, (WD.txid, WD.vin_amount, WD.fee, 0, 0))

                    for i, out in enumerate(WD.vout):
                        if out.change:
                            await cur.execute(q2, (WD.txid, i, out.amount, out.userid, out.public_key))
                            continue
                        await cur.execute(q3, (out.k1, WD.txid, i, out.amount, out.public_key))

                    for vin in WD.vin:
                        await cur.execute(q4, (WD.txid, vin.txid, vin.vout, vin.amount, vin.public_key))
                        # await cur.execute(q7, (vin.txid, vin.vout, ))

                    for req in WD.user_requests.values():
                        await cur.execute(q5, (req.k1, ))

    async def create_withdraw_transaction(self, n: int, WD: WithdrawalModel):
        q = """
        UPDATE btc_payments
        SET confirmations = %s
        WHERE txid_hex = %s
        """        
        q1 = """
        INSERT INTO withdraw_transactions
        (userid, network, txid_hex, vout, amount, fee, ts_created)
        VALUES (%s, 'BTC', %s, %s, %s, %s, %s)
        """
        q2 = """
        DELETE FROM locked_balances
        WHERE k1 = %s
        """
        q3 = """
        UPDATE withdraw_requests
        SET status = 'PAID'
        WHERE k1 = %s
        """
        q5 = """
        INSERT INTO utxos
        (userid, public_key, txid_hex, vout, amount, locked, ts_created)
        VALUES(%s, %s, %s, %s, %s, %s, %s)
        """
        q7 = """
        UPDATE wallet_addresses
        SET used = used + 1
        WHERE script_pubkey = %s
        """        
        q6 = """
        DELETE from utxos
        WHERE txid_hex = %s
        AND vout = %s
        """                
        current_time = int(datetime.utcnow().timestamp())
        async with self.pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                async with conn.transaction():
                    await cur.execute(q, (n, WD.txid, ))
                    for i, out in enumerate(WD.vout):
                        if out.change: 
                            await cur.execute(q5, (out.userid, out.public_key, WD.txid, i, out.amount, 0, current_time))
                            await cur.execute(q7, (out.public_key, ))
                            continue
                        fee = WD.user_requests[out.userid].request_amount - out.amount
                        await cur.execute(q1, (out.userid, WD.txid, i, out.amount, fee, current_time, ))                        
                        await cur.execute(q2, (out.k1, ))
                        await cur.execute(q3, (out.k1, ))
                    for i, vin in enumerate(WD.vin):
                        await cur.execute(q6, (vin.txid, vin.vout, ))

    async def unlock_utxos(self, utxos: [tuple[str, int]]):
        q = """
        UPDATE utxos
        SET locked = 0
        WHERE txid_hex = %s
        AND vout = %s
        """
        async with self.pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                async with conn.transaction():
                    for u in utxos:
                        await cur.execute(q, (u[0], u[1]))
    
    async def lock_withdraw_balances(self, WD: WithdrawalModel):
        q = """
        UPDATE balances
        SET amount = amount - %s
        WHERE userid = %s
        AND market = 'BTC'
        """
        q2 = """
        INSERT INTO locked_balances(txid_hex, vout, userid, amount)
        VALUES (%s, %s, %s, %s)
        """
        q3 = """
        INSERT INTO btc_payments
        (userid, k1, txid_hex, vout, address, amount, fee_sat, ts_create)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        q4 = """
        UPDATE withdraw_requests
        SET txid_hex = %s,
        redeemed = TRUE,
        vout = %s,
        ts_paid = %s,
        status = "PAID"
        """
        current_time = int(datetime.utcnow().timestamp())
        async with self.pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                async with conn.transaction():
                    for i, vout in enumerate(WD.vout):
                        if vout.change: continue
                        request = WD.user_requests[vout[2]]
                        await cur.execute(q, (request.request_amount, request.userid))
                        await cur.execute(q2, (WD.txid, i, request.userid, request.request_amount))
                        await cur.execute(q3, (request.userid, request.k1, WD.txid, i, request.public_key, request.request_amount, request.fee, current_time))
                        await cur.execute(q4, (WD.txid, i, current_time))


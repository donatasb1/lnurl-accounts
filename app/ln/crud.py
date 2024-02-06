from datetime import datetime
import asyncio
import psycopg_pool
import psycopg
from psycopg.rows import dict_row
from ..user.base import WithdrawRequest
from ..user.crud import PSQLClient
from .base import LNDInvoice, LNPayment, PaymentStatus


def lock_function(func):
    async def wrapper(*args, **kwargs):
        async with asyncio.Lock():
            return await func(*args, **kwargs)
    return wrapper


class LNCrud(PSQLClient):

    def __init__(self, pool: psycopg_pool.AsyncConnectionPool):
        super().__init__(pool)

    """
    WITHDRAW
    """

    async def get_user_k1(self, userid: str) -> str | None:
        q = """
        SELECT k1
        FROM users
        WHERE userid = %s
        """
        k1 = await self.fetchone(q, userid)
        if k1 is not None:
            return k1.get("k1", None)
        return None

    async def get_user_by_k1(self, k1: str) -> int:
        q = """
        SELECT COUNT(userid) exists
        FROM users
        WHERE k1 = %s
        """
        userid = await self.fetchone(q, k1)
        return userid.get('exists', 0)
    
    async def get_user_balances(self, userid: str) -> int:
        q = """
        SELECT amount 
        FROM balances
        WHERE userid = %s
        AND market = 'usd'
        """
        balances = await self.fetchone(q, userid)
        return balances.get("amount", 0)

    async def get_withdraw_request(self, k1: str) -> WithdrawRequest | None:
        q = """
        SELECT * 
        FROM withdraw_requests
        WHERE k1 = %s
        """
        row = await self.fetchone(q, k1)
        if row is not None:
            return WithdrawRequest(**row)
        
    async def get_user_pending_requests(self, userid: str) -> int:
        q1 = """
        UPDATE withdraw_requests
        SET status = 'EXPIRED'
        WHERE ts_created < %s
        AND status = 'CREATED'
        """
        q2 = """
        SELECT COUNT(k1) as pending
        FROM withdraw_requests
        WHERE userid = %s
        AND status NOT IN ('PAID', 'SETTLED', 'EXPIRED', 'REJECTED', 'PAYMENT_FAILED')
        """
        ts = int(datetime.utcnow().timestamp() - 60*5)
        await self.execute(q1, ts - 60*5)
        count_requests = await self.fetchone(q2, userid)
        return count_requests.get("pending", 0)

    async def create_withdraw_request(self, request: WithdrawRequest) -> None:
        q = """
        INSERT INTO withdraw_requests 
            (
                userid, k1, network, status, ts_created
            )
        VALUES (%s, %s, %s, %s, %s)
        """
        return await self.execute(q, request.userid, request.k1, request.network,
                                  request.status, request.ts_created)

    async def update_withdraw_status(self, status: str, k1: str = None, hash: str = None, reason: str = "") -> None:
        if hash is not None:
            q = f"""
            UPDATE withdraw_requests
            SET status = %s,
            reason = %s
            WHERE txid_hex = '{hash}'
            """
        elif k1 is not None:
            q = f"""
            UPDATE withdraw_requests
            SET status = %s,
            reason = %s
            WHERE k1 = '{k1}'
            """
        return await self.execute(q, status, reason)

    async def get_latest_ln_payment(self) -> int:
        q = """
        SELECT MAX(timestamp) ts
        FROM withdraw_invoices
        """
        max_ts = await self.fetchone(q)
        if max_ts is not None:
            return max_ts.get("ts", 0)
        return 0
    
    async def get_max_issued_add_index(self) -> int:
        q = """
        SELECT MAX(add_index) idx
        FROM deposit_invoices
        WHERE state IN ('SETTLED', 'CANCELED')
        """
        idx = await self.fetchone(q)
        if idx is not None:
            idx.get("idx", 0)
        return 0

    async def get_withdraw_invoice_exists(self, payment: LNPayment) -> int:
        q = """
        SELECT COUNT(payment_hash) exists
        FROM withdraw_invoices
        WHERE payment_hash = %s
        """
        exists = await self.fetchone(q, payment.payment_hash)
        return exists.get("exists")

    @lock_function
    async def withdraw_redeem_request(self, k1: str, invoice: LNDInvoice):
        q1 = """
        SELECT *
        FROM withdraw_requests
        WHERE k1 = %s
        AND status = 'VERIFIED'
        """
        q2 = """
        UPDATE withdraw_requests
        SET redeemed = TRUE,
        amount = %s,
        destination = %s,
        status = 'IN_FLIGHT'
        WHERE k1 = %s
        """
        q3 = """
        UPDATE balances
        SET amount = balances.amount -%s
        FROM withdraw_requests
        WHERE balances.userid = withdraw_requests.userid
        AND withdraw_requests.k1 = %s
        """
        q4 = """
        INSERT INTO locked_balances
        (userid, k1, amount)
        SELECT userid, %s, %s
        FROM withdraw_requests
        WHERE k1 = %s
        """
        q5 = """
        INSERT INTO withdraw_invoices
            (
                k1, payment_hash, bolt11, state, destination, num_satoshis, timestamp, expiry, description,
                description_hash, fallback_addr, cltv_expiry, route_hints, payment_addr, features, add_index
            )
        VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """        
        q6 = """
        INSERT INTO ln_payments 
        (k1, userid, payment_hash, value_sat, ts_created)
        VALUES (%s, %s, %s, %s, %s)
        """        
        current_time = int(datetime.utcnow().timestamp())
        async with self.pool.connection() as conn:
            await conn.set_autocommit(True)
            async with conn.cursor(row_factory=dict_row) as cur:
                cur: psycopg.Cursor
                await cur.execute(q1, (k1, ))
                request = await cur.fetchone()
                if request is None:
                    return None
                async with conn.transaction():
                    await cur.execute(q2, (invoice.num_satoshis, invoice.destination, k1))
                    await cur.execute(q3, (invoice.num_satoshis, k1))
                    await cur.execute(q4, (k1, invoice.num_satoshis, k1))
                    await cur.execute(q5, (k1, invoice.payment_hash, invoice.bolt11, invoice.state,
                                            invoice.destination, invoice.num_satoshis, invoice.timestamp,
                                            invoice.expiry, invoice.description, invoice.description_hash,
                                            invoice.fallback_addr, invoice.cltv_expiry, invoice.route_hints,
                                            invoice.payment_addr, invoice.features, invoice.add_index, ))
                    await self.execute(q6, request['k1'], request["userid"], invoice.payment_hash, invoice.num_satoshis, current_time)
        return WithdrawRequest(**request)


    async def create_withdraw_invoice(self, invoice: LNDInvoice) -> None:
        q = """
        INSERT INTO withdraw_invoices
            (
                payment_hash, bolt11, state, destination, num_satoshis, timestamp, expiry, description,
                description_hash, fallback_addr, cltv_expiry, route_hints, payment_addr, features, add_index
            )
        VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        return await self.execute(q, (invoice.payment_hash, invoice.bolt11, invoice.state,
                                      invoice.destination, invoice.num_satoshis, invoice.timestamp,
                                      invoice.expiry, invoice.description, invoice.description_hash,
                                      invoice.fallback_addr, invoice.cltv_expiry, invoice.route_hints,
                                      invoice.payment_addr, invoice.features, invoice.add_index,
                                      ))

    async def create_withdraw_transaction(self, request: WithdrawRequest):
        q = """
        INSERT INTO withdraw_transactions (userid, network, txid_hex, amount, fee, ts_created)
        VALUES (%s, %s, %s)
        ON CONFLICT DO NOTHING
        """
        return await self.execute(request.userid, "LN", request.payment_hash, request.amount, request.ts_created)

    async def create_ln_payment(self, request: dict, invoice: LNDInvoice):
        q = """
        INSERT INTO ln_payments (k1, userid, payment_hash, value_sat, ts_created)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING
        """
        current_time = int(datetime.utcnow().timestamp())
        return await self.execute(q, request['k1'], request["userid"], invoice.payment_hash, invoice.num_satoshis, current_time)    

    async def finalize_payment(self, payment: PaymentStatus):
        q = """
        UPDATE ln_payments
        SET fee_sat = %s,
        status = %s
        WHERE payment_hash = %s
        """
        q2 = """
        DELETE FROM locked_balances
        USING ln_payments
        WHERE locked_balances.k1 = ln_payments.k1
        AND ln_payments.payment_hash = %s
        """
        q3 = """
        INSERT INTO withdraw_transactions 
        (txid_hex, vout, network, userid, amount, fee, ts_created)
        SELECT %s, 0, 'LN', lp.userid, %s, %s, %s
        FROM ln_payments AS lp
        WHERE lp.payment_hash = %s
        """
        q4 = """
        UPDATE withdraw_requests AS wr
        SET status = 'PAID'
        FROM ln_payments AS lp
        WHERE wr.k1 = lp.k1
        AND lp.payment_hash = %s
        """
        q5 = """
        UPDATE withdraw_invoices
        SET preimage = %s,
        state = %s
        WHERE payment_hash = %s
        """
        current_time = int(datetime.utcnow().timestamp())
        async with self.pool.connection() as conn:
            await conn.set_autocommit(True)
            async with conn.cursor(row_factory=dict_row) as cur:
                async with conn.transaction():
                    await cur.execute(q, (payment.fee_sat, payment.status, payment.payment_hash))
                    await cur.execute(q2, (payment.payment_hash, ))
                    await cur.execute(q3, (payment.payment_hash, payment.value_sat, payment.fee_sat, current_time, payment.payment_hash))
                    await cur.execute(q4, (payment.payment_hash, ))
                    await cur.execute(q5, (payment.payment_preimage, payment.status, payment.payment_hash, ))
                return

    async def get_payment_exists(self, payment: PaymentStatus) -> dict:
        q = """
        SELECT COUNT(payment_hash) exists
        FROM ln_payments
        WHERE status IN ('FAILED', 'SUCCEEDED')
        AND payment_hash = %s
        """
        exists = await self.fetchone(q, payment.payment_hash)
        return exists.get('exists', 0)

    async def failed_payment(self, payment: PaymentStatus) -> dict:
        q = """
        UPDATE ln_payments
        SET status = 'FAILED'
        WHERE payment_hash = %s
        """
        q2 = """
        DELETE FROM locked_balances
        USING ln_payments
        WHERE locked_balances.k1 = ln_payments.k1
        AND ln_payments.payment_hash = %s
        """
        q3 = """
        UPDATE balances
        SET amount = balances.amount + lp.value_sat
        FROM ln_payments AS lp
        WHERE balances.userid = lp.userid
        AND lp.payment_hash = %s
        AND lp.status NOT IN ('SUCCEEDED', 'FAILED')
        """
        q4 = """
        UPDATE withdraw_requests
        SET status = 'PAYMENT_FAILED'
        FROM ln_payments AS lp
        WHERE withdraw_requests.k1 = lp.k1
        AND lp.payment_hash = %s
        """
        q5 = """
        SELECT wr.userid
        FROM withdraw_requests AS wr
        JOIN ln_payments AS lp
        ON lp.k1 = wr.k1
        WHERE lp.payment_hash = %s
        AND lp.status NOT IN ('SUCCEEDED', 'FAILED')
        """
        async with self.pool.connection() as conn:
            await conn.set_autocommit(True)
            async with conn.cursor(row_factory=dict_row) as cur:
                async with conn.transaction():
                    await cur.execute(q3, (payment.payment_hash, ))                    
                    await cur.execute(q5, (payment.payment_hash, ))
                    userid = await cur.fetchone()                    
                    await cur.execute(q2, (payment.payment_hash, ))
                    await cur.execute(q4, (payment.payment_hash, ))
                await cur.execute(q, (payment.payment_hash, ))
                return userid
    
    """
    DEPOSIT
    """

    async def create_deposit_invoice(self, invoice: LNDInvoice):
        q = """
        INSERT INTO deposit_invoices
            (
                k1, payment_hash, bolt11, state, 
                destination, num_satoshis, timestamp, 
                expiry, description, description_hash, 
                fallback_addr, cltv_expiry, route_hints, 
                payment_addr, features, add_index
            )
        VALUES(
            %s, %s, %s, %s,
            %s, %s, %s, 
            %s, %s, %s, 
            %s, %s, %s, 
            %s, %s, %s)
        """
        return await self.execute(q, invoice.k1, invoice.payment_hash, invoice.bolt11, invoice.state,
                                    invoice.destination, invoice.num_satoshis, invoice.timestamp,
                                    invoice.expiry, invoice.description, invoice.description_hash,
                                    invoice.fallback_addr, invoice.cltv_expiry, invoice.route_hints,
                                    invoice.payment_addr, invoice.features, invoice.add_index,
                                )

    async def create_deposit_transaction(self, invoice: LNDInvoice):
        q = """
        INSERT INTO deposit_transactions
        (userid, payment_hash, amount, ts_created)
        VALUES (%s, %s, %s, %s)
        """
        return await self.execute(q, invoice)
    
    async def deposit_finalize(self, invoice: LNDInvoice) -> str | None:
        q = """
        UPDATE deposit_invoices
        SET state = %s
        WHERE payment_hash = %s
        """
        q2 = """
        INSERT INTO deposit_transactions
        (userid, network, txid_hex, vout, amount, ts_created)
        SELECT u.userid, 'LN', %s, 0, %s, %s
        FROM deposit_invoices AS di
        JOIN users AS u
        ON di.k1 = u.k1
        WHERE di.payment_hash = %s
        """
        q3 = """
        UPDATE balances AS b
        SET amount = b.amount + %s
        WHERE market = 'usd'
        AND userid = (
            SELECT u.userid 
            FROM users AS u
            JOIN deposit_invoices AS di
            ON u.k1 = di.k1
            WHERE di.payment_hash = %s
            )
        """
        q4 = """
        SELECT u.userid
        FROM users AS u
        JOIN deposit_invoices AS di
        ON u.k1 = di.k1
        WHERE di.payment_hash = %s
        AND di.state NOT IN ('SETTLED', 'CANCELED')
        """
        current_time = int(datetime.utcnow().timestamp())
        async with self.pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                async with conn.transaction():
                    await cur.execute(q4, (invoice.payment_hash, ))
                    userid = await cur.fetchone()                    
                    await cur.execute(q, (invoice.state, invoice.payment_hash, ))
                    await cur.execute(q2, (invoice.payment_hash, invoice.num_satoshis, current_time, invoice.payment_hash, ))
                    await cur.execute(q3, (invoice.num_satoshis, invoice.payment_hash, ))
                if userid is not None:
                    return userid.get("userid")
                return userid
            
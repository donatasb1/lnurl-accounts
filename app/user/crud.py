import psycopg_pool
from datetime import datetime, timezone
from psycopg.rows import dict_row
from ..database import PSQLClient
from ..btc.base import WalletAddressInDb
from .auth import derive_new_address
from .base import WithdrawRequest, UserInDB, OrderSYS, Transfer


class BaseCrud(PSQLClient):
    """
    USER sql cmds
    """
    def __init__(self, pool: psycopg_pool.AsyncConnectionPool):
        super().__init__(pool)

    async def check_email_exists(self, email: str) -> int:
        q = """
        SELECT COUNT(userid) exists
        FROM users
        WHERE email = %s
        """
        exists = await self.fetchone(q, email)
        return exists.get("exists", 0)
    
    async def check_username_exists(self, username: str) -> int:
        q = """
        SELECT COUNT(userid) exists
        FROM users
        WHERE username = %s
        """
        exists = await self.fetchone(q, username)
        return exists.get("exists", 0)
    
    async def create_user(self, user: UserInDB):
        q = """
        INSERT INTO users
        (userid, username, email, hashed_password, k1)
        VALUES (%s, %s, %s, %s, %s)
        """
        await self.execute(q, user.userid, user.username, str(user.email), user.hashed_password, user.k1)

    async def create_feerates(self, user: UserInDB):
        q = """
        INSERT INTO feerates
        (userid, network, rate, policy)
        VALUES (%s, 'BTC', 1.0, 'default'), (%s, 'LN', 1.0, 'default')
        """
        await self.execute(q, user.userid, user.userid)

    async def create_balances(self, user: UserInDB):
        q = """
        INSERT INTO balances
        (userid, market, market_name, amount)
        VALUES (%s, 'usd', 'usd', 0)
        """
        await self.execute(q, user.userid)

    async def create_user_entry(self, user: UserInDB):
        q = """
        INSERT INTO users
        (userid, username, email, hashed_password, k1)
        VALUES (%s, %s, %s, %s, %s)
        """
        q2 = """
        INSERT INTO feerates
        (userid, network, rate, policy)
        VALUES (%s, 'BTC', 1.0, 'default'), (%s, 'LN', 1.0, 'default')
        """
        q3 = """
        INSERT INTO balances
        (userid, market, market_name, amount)
        VALUES (%s, 'usd', 'usd', 0)
        """
        async with self.pool.connection() as conn:
            await conn.set_autocommit(True)
            async with conn.cursor(row_factory=dict_row) as cur:
                async with conn.transaction():
                    await cur.execute(q, (user.userid, user.username, str(user.email), user.hashed_password, user.k1))
                    await cur.execute(q2, (user.userid, user.userid, ))
                    await cur.execute(q3, (user.userid, ))


    async def log_user_event(self, event):
        q = """
        INSERT INTO userlog
        (ts, userid, event, body, client_host)
        VALUES(%s, %s, %s, %s, %s)
        """
        now = int(datetime.now(timezone.utc).timestamp())
        await self.execute(q, now, event["userid"], event["event"], event["body"], event["host"])

    async def get_user(self, username: str) -> None | UserInDB:
        q = """
        SELECT *
        FROM users
        WHERE username = %s
        """
        user = await self.fetchone(q, username)
        if user is None:
            return None
        return UserInDB(**user)
    
    async def get_user_orders(self, userid: str) -> list[OrderSYS]:
        q = """
        SELECT orderid, tsid, userid, market, side, market_name, market_expiry, side_name, price, size, fill
        FROM orders
        WHERE userid = %s
        """
        orders = await self.fetchmany(q, userid)
        return [OrderSYS(**o) for o in orders]
    
    async def get_user_balances(self, userid: str) -> dict:
        q = """
        SELECT market, amount 
        FROM balances
        WHERE userid = %s
        """
        balances = await self.fetchmany(q, userid)
        return {b["market"]: b["amount"] for b in balances}

    async def get_user_transfers(self, userid: str, limit: int) -> list[Transfer]:
        q = """
        SELECT sub.*
        FROM (
            SELECT 'Withdrawal' as txtype, userid, network, txid_hex, vout, amount, fee, ts_created
            FROM withdraw_transactions
            WHERE userid = %s
            UNION ALL
            SELECT 'Deposit' as txtype, userid, network, txid_hex, vout, amount, 0 as fee, ts_created
            FROM deposit_transactions
            WHERE userid = %s        
        ) AS sub
        ORDER BY sub.ts_created DESC
        LIMIT %s
        """
        transfers = await self.fetchmany(q, userid, userid, limit)
        return [Transfer(**t) for t in transfers]

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

    async def get_unused_address(self, userid: str):
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
        # print('got next addressindex', idx)
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

    async def get_withdraw_request(self, k1: str) -> WithdrawRequest:
        q = """
        SELECT * 
        FROM withdraw_requests
        WHERE k1 = %s
        """
        row = await self.fetchone(q, k1)
        if row is not None:
            return WithdrawRequest(**row)    

    async def get_active_withdraw_requests(self, userid: str) -> list[WithdrawRequest]:
        q = """
        SELECT *
        FROM withdraw_requests
        WHERE userid = %s
        AND status IN ('CREATED', 'VERIFIED', 'QUEUED')
        """        
        requests = await self.fetchmany(q, userid)
        return [WithdrawRequest(**r) for r in requests]

    async def get_all_withdraw_requests(self, userid) -> list[WithdrawRequest]:
        q = """
        SELECT *
        FROM withdraw_requests
        WHERE userid = %s
        ORDER BY ts_created DESC
        LIMIT 50
        """
        requests = await self.fetchmany(q, userid)
        return [WithdrawRequest(**r) for r in requests]
    
    async def cancel_withdraw_request(self, k1: str):
        q = """
        WITH deleted_rows AS (
            DELETE FROM locked_balances
            WHERE k1 = %s
            RETURNING userid, amount
        )
        UPDATE balances b
        SET amount = b.amount + deleted_rows.amount
        FROM deleted_rows
        WHERE b.userid = deleted_rows.userid
        AND b.market = 'usd'
        """
        q2 = """
        UPDATE withdraw_requests
        SET status = 'REJECTED',
        reason = 'User canceled'
        WHERE k1 = %s
        AND status IN ('CREATED', 'VERIFIED', 'QUEUED')
        """

        async with self.pool.connection() as conn:
            await conn.set_autocommit(True)
            async with conn.cursor(row_factory=dict_row) as cur:
                async with conn.transaction():
                    await cur.execute(q, (k1, ))
                    await cur.execute(q2, (k1 ,))                    

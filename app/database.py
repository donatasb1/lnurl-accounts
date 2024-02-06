"""
PSQL Async Pool Client
"""

import psycopg_pool
from psycopg.rows import dict_row
import os
import asyncio
from .connections import logger

class PSQLClient:

    def __init__(self, pool: psycopg_pool.AsyncConnectionPool):
        self.pool = pool
    
    async def execute(self, q: str, *args):
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(q, args)

    async def execute_many(self, q: str, *args):
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.executemany(q, args)

    async def fetchone(self, q: str, *args) -> dict:
        async with self.pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(q, args)
                row = await cur.fetchone()
                return row
    
    async def fetchmany(self, q: str, *args) -> list[dict]:
        async with self.pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(q, args)
                rows = await cur.fetchall()
                return rows


async def db_init(pool):
    if os.getenv("DROP_DATABASE"):
        await asyncio.gather(_drop_tables(pool, all_tables))
    if os.getenv("INIT_DATABASE"):
        await asyncio.gather(_init_tables_all(pool))


async def _drop_tables(pool, tables):
    tables_str = ", ".join(tables)
    logger.debug("Dropping tables: "+tables_str)
    async with pool.connection() as conn:
        await conn.set_autocommit(True)
        async with conn.cursor() as cur:
            for table in tables:
                q = f"""
                DROP TABLE IF EXISTS {table};
                """                        
                await cur.execute(q)
    logger.debug("Tables dropped successfully")


async def _init_tables_all(pool):
    logger.debug("Initializing database tables")
    async with pool.connection() as conn:
        await conn.set_autocommit(True)
        async with conn.cursor() as cur:
            # base
            await create_users_table(cur)
            await create_balances_table(cur)
            await create_userlog_table(cur)
            await create_feerates_table(cur)
            # transfers
            await create_deposit_transactions_table(cur)
            await create_withdraw_transactions_table(cur)
            await create_withdraw_requests_table(cur)
            await create_locked_balances_table(cur)
            # ln
            await create_withdraw_invoices_table(cur)
            await create_ln_payments_table(cur)
            await create_deposit_invoices_table(cur)
            # btc
            await create_wallet_addresses_table(cur)
            await create_utxos_table(cur)
            await create_btc_payments_table(cur)
            await create_change_outs_table(cur)
            await create_wd_outs_table(cur)
            await create_wd_ins_table(cur)
    logger.debug("Initializing database tables")

base = [
    "users", 
    "balances", 
    "userlog", 
    'feerates']

deposits = [
    'deposit_transactions', 
    'withdraw_transactions',
    'withdraw_requests', 
    'locked_balances']

ln = [
    'withdraw_invoices', 
    'ln_payments', 
    'deposit_invoices']

btc = [
    'wallet_addresses', 
    'utxos', 
    'btc_payments', 
    'change_outs', 
    'wd_outs', 
    'wd_ins']

all_tables = base+deposits+ln+btc


async def create_users_table(cursor):
    q = """
    CREATE TABLE IF NOT EXISTS users
    (
        username character varying(100) NOT NULL,
        userid character(64) NOT NULL PRIMARY KEY,
        email character varying(100) NOT NULL,
        hashed_password character varying(100) NOT NULL,
        k1 character(64) NOT NULL
    )
    """
    await cursor.execute(q)


async def create_balances_table(cursor):
    q = """
    CREATE TABLE IF NOT EXISTS balances
    (
        userid character(64) NOT NULL,
        market character varying(50) NOT NULL,
        market_name character varying(100) NOT NULL,
        amount bigint DEFAULT 0,
        PRIMARY KEY (userid, market)
    )
    """
    await cursor.execute(q)


async def create_userlog_table(cursor):
    q = """
    CREATE TABLE IF NOT EXISTS userlog
    (
        ts bigint NOT NULL,
        userid character(64) NOT NULL,
        event character varying(50) NOT NULL,
        body character varying(500),
        client_host character varying(100),
        PRIMARY KEY (ts, userid)
    )
    """
    await cursor.execute(q)


async def create_feerates_table(cursor):
    q = """
    CREATE TABLE IF NOT EXISTS feerates
    (
        userid character varying(100) NOT NULL,
        network character varying(20) NOT NULL,
        rate double precision DEFAULT 1.0,
        policy character varying(100),
        PRIMARY KEY (userid, network)
    )
    """
    await cursor.execute(q)

"""
TRANSFERS
"""

async def create_deposit_transactions_table(cursor):
    q = """
    CREATE TABLE IF NOT EXISTS deposit_transactions
    (
        userid character (64) NOT NULL,
        network character varying (5) NOT NULL,
        txid_hex character(64) NOT NULL,
        vout bigint DEFAULT 0 NOT NULL,
        amount bigint NOT NULL,
        ts_created bigint NOT NULL,
        PRIMARY KEY (txid_hex, vout)
    )
    """
    await cursor.execute(q)

async def create_withdraw_transactions_table(cursor):
    q = """
    CREATE TABLE IF NOT EXISTS withdraw_transactions
    (
        userid character(64) NOT NULL,
        network character varying (5) NOT NULL,
        txid_hex character(64) NOT NULL,
        vout bigint NOT NULL,
        amount bigint NOT NULL,
        fee bigint NOT NULL,
        ts_created bigint NOT NULL,
        PRIMARY KEY (txid_hex, vout)
    )
    """
    await cursor.execute(q)

async def create_withdraw_requests_table(cursor):
    q = """
    CREATE TABLE IF NOT EXISTS withdraw_requests
    (
        k1 character(64) NOT NULL PRIMARY KEY,
        userid character(64) NOT NULL,
        network character varying (5) NOT NULL,

        redeemed boolean DEFAULT FALSE, 
        status character varying(20) NOT NULL, 
        reason character varying(300),

        amount bigint,
        destination character varying(132),

        ts_created bigint NOT NULL
    )
    """
    await cursor.execute(q)

async def create_locked_balances_table(cursor):
    q = """
    CREATE TABLE IF NOT EXISTS locked_balances
    (
        userid character(64) NOT NULL,
        k1 character(64) NOT NULL PRIMARY KEY,
        amount bigint NOT NULL
    )
    """
    await cursor.execute(q)


"""
LIGHTNING
"""

async def create_withdraw_invoices_table(cursor):
    q = """
    CREATE TABLE IF NOT EXISTS withdraw_invoices
    (
        k1 character(64) NOT NULL,
        payment_hash character(64) NOT NULL PRIMARY KEY,
        bolt11 character varying(1023) NOT NULL,

        state character varying(20) NOT NULL,
        preimage character (64),

        destination character varying (132) NOT NULL,
        num_satoshis bigint NOT NULL,
        timestamp bigint NOT NULL,
        add_index bigint,
        expiry bigint NOT NULL,
        description character varying (1023),
        description_hash character varying (1023),
        fallback_addr character varying (132),
        cltv_expiry bigint,
        route_hints text,
        payment_addr character varying(132),
        features text
    )
    """
    await cursor.execute(q)


async def create_ln_payments_table(cursor):
    q = """
    CREATE TABLE IF NOT EXISTS ln_payments
    (
        k1 character(64) NOT NULL,
        userid character(64) NOT NULL,
        payment_hash character(64) PRIMARY KEY NOT NULL,
        value_sat bigint,
        status character varying (20) DEFAULT 'CREATED',
        fee_sat bigint,
        ts_created bigint NOT NULL,
        failure_reason text
    )
    """
    await cursor.execute(q)


async def create_deposit_invoices_table(cursor):
    q = """
    CREATE TABLE IF NOT EXISTS deposit_invoices
    (
        k1 character(64) NOT NULL,
        payment_hash character(64) NOT NULL PRIMARY KEY,
        bolt11 character varying(1023) NOT NULL,
        state character varying(20) NOT NULL,
        preimage character (64),
        destination character varying (132) NOT NULL,
        num_satoshis bigint NOT NULL,
        timestamp bigint NOT NULL,
        add_index bigint,        
        expiry bigint NOT NULL,
        description character varying (1023),
        description_hash character varying (1023),
        fallback_addr character varying (132),
        cltv_expiry bigint,
        route_hints text,
        payment_addr character varying(132),
        features text
    )
    """
    await cursor.execute(q)

"""
MAINNET
"""

async def create_wallet_addresses_table(cursor):
    q = """
    CREATE TABLE IF NOT EXISTS wallet_addresses
    (
        public_key character varying (132) NOT NULL PRIMARY KEY,
        chain_code character varying (132) NOT NULL,
        user_index bigint NOT NULL,
        userid character varying(100) NOT NULL,
        change bigint NOT NULL,
        address_index bigint NOT NULL,
        used bigint DEFAULT 0,
        path character varying (20) NOT NULL,
        witness_script text NOT NULL,
        script_pubkey character varying(200) NOT NULL,
        p2wsh character (64) NOT NULL
    )
    """
    await cursor.execute(q)


async def create_utxos_table(cursor):
    q = """
    CREATE TABLE IF NOT EXISTS utxos
    (
        userid character(64) NOT NULL,
        public_key character varying (132) NOT NULL,
        txid_hex character (64) NOT NULL,
        vout bigint NOT NULL,
        amount bigint NOT NULL,
        locked bigint,
        ts_created bigint,
        PRIMARY KEY (txid_hex, vout)
    )
    """
    await cursor.execute(q)

async def create_btc_payments_table(cursor):
    q = """
    CREATE TABLE IF NOT EXISTS btc_payments
    (   
        txid_hex character(64) NOT NULL PRIMARY KEY,
        amount bigint NOT NULL,
        fee bigint NOT NULL,
        fee_covered bigint NOT NULL,
        confirmations bigint DEFAULT 0
    )
    """
    await cursor.execute(q)

async def create_change_outs_table(cursor):
    q = """
    CREATE TABLE IF NOT EXISTS change_outs
    (   
        txid_hex character(64) NOT NULL PRIMARY KEY,
        vout bigint NOT NULL,
        amount bigint NOT NULL,
        userid character(64) NOT NULL,
        public_key character varying (132) NOT NULL
    )
    """
    await cursor.execute(q)

async def create_wd_outs_table(cursor):
    q = """
    CREATE TABLE IF NOT EXISTS wd_outs
    (
        k1 character(64) NOT NULL PRIMARY KEY,
        txid_hex character(64) NOT NULL,
        vout bigint NOT NULL,
        amount bigint NOT NULL,
        public_key character varying (132) NOT NULL
    )
    """
    await cursor.execute(q)

async def create_wd_ins_table(cursor):
    q = """
    CREATE TABLE IF NOT EXISTS wd_ins
    (
        txid_hex character(64) NOT NULL,
        txid_hex_prev character(64) NOT NULL,
        vout bigint NOT NULL,
        amount bigint NOT NULL,
        public_key character varying (132) NOT NULL,
        PRIMARY KEY (txid_hex_prev, vout)
    )
    """
    await cursor.execute(q)

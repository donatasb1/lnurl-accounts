from ..connections import get_redis_connection, logger, node, psql_pool
from .crud import LNCrud
from .base import PaymentStatus
import psycopg.errors


psql = LNCrud(psql_pool)


async def load_deposit_invoices():
    max_index = await psql.get_max_issued_add_index()
    all_payments = await node.scan_invoices(max_index)
    for invoice in all_payments:
        if invoice.state == "SETTLED":
            try:
                # get userid associated with payment_hash
                userid = await psql.deposit_finalize(invoice)
                if userid is None:
                    continue
                # cache balance transaction
                async for redis_conn in get_redis_connection():
                    redis_conn.hincrby(userid+"::session", "balances", invoice.num_satoshis)
            # abort transaction if deposit already redeemed
            except psycopg.errors.IntegrityError as e:
                pass


async def load_ln_payments():
    # get latest payments
    latest_ts = await psql.get_latest_ln_payment()
    all_payments = await node.list_payments(latest_ts)
    for payment in all_payments:
        # check if payment belongs to withdraw invoice
        exists = await psql.get_withdraw_invoice_exists(payment)
        # payment without associated invoice
        if not exists:
            logger.info({"event": "outgoing_payment", "body": {"payment_hash": payment.payment_hash, "msg": "No invoice"}})
            continue
        status = PaymentStatus(
            payment_hash=payment.payment_hash,
            payment_preimage=payment.payment_preimage,
            value_sat=payment.value_sat,
            status=payment.status,
            fee_sat=payment.fee_sat)
        exists = await psql.get_payment_exists(status)
        if exists: continue        
        if status.status == "SUCCEEDED":
            # move amount from locked_balances to withdraw_transaction using payment_hash
            try:
                await psql.finalize_payment(status)
            except psycopg.errors.IntegrityError as e:
                logger.exception({"error": "Duplicate payment entry. Failed", "payment_hash": status.payment_hash})
        elif status.status == "FAILED":
            userid = await psql.failed_payment(status)
            if userid is not None:
                async for redis_conn in get_redis_connection():
                    redis_conn.hincrby(userid+"::session", "balances", status.value_sat)


async def process_payment_notifications():
    async for status in node.track_payments():
        exists = await psql.get_payment_exists(status)
        if exists:
            continue        
        if status.status == "SUCCEEDED":
            try:
                await psql.finalize_payment(status)
            except psycopg.errors.IntegrityError as e:
                logger.error({"error": "Duplicate ln payment entry", "payment_hash": status.payment_hash})
        elif status.status == "FAILED":
            logger.error({"error": "Failed ln payment", "payment_hash": status.payment_hash})            
            # move from locked to owned if status not FAILED or SUCCEEDED
            userid = await psql.failed_payment(status)
            if userid is not None:
                # if userid not redeemed increase cache balance by failed amount
                async for redis_conn in get_redis_connection():
                    redis_conn.hincrby(userid+"::session", "balances", status.value_sat)
            else:
                logger.error({"error": "Duplicate ln payment entry. Failed", "payment_hash": status.payment_hash})


async def process_invoice_notifications():
    # get index of latest invoice that has status finalized
    await load_deposit_invoices()
    idx = await psql.get_max_issued_add_index()
    # Deposit notifications
    # tracks issued invoices
    # notification on create, and state updates
    # by providing add_index can catch up to missed invoice updates
    idx = 0
    async for invoice in node.paid_invoices_stream(idx):
        if invoice.state == "SETTLED":
            try:
                # get userid associated with payment_hash
                userid = await psql.deposit_finalize(invoice)
                if userid is None:
                    continue
                async for redis_conn in get_redis_connection():
                    redis_conn.hincrby(userid+"::session", "balances", invoice.num_satoshis)
            # abort transaction if deposit already redeemed
            except psycopg.errors.IntegrityError as e:
                logger.error({"error": "Duplicate deposit entry", "payment_hash": invoice.payment_hash})

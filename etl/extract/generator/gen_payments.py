# etl/extract/generator/gen_payments.py
# ============================================================
# Generates and inserts fact_payments rows.
# total ~1,10,000 payments
# ============================================================

import random
from datetime import date, timedelta

from .config import (
    PAYMENT_PROVIDERS, PAYMENT_METHODS,
    PAYMENT_METHOD_WEIGHTS, GATEWAY_CODES 
)
from .db import bulk_insert, date_to_sk, random_datetime_between


def generate_payments(conn, updated_orders_map):
    """
    Generates and inserts fact_payments rows.
    total 1,00,000 payments
    """

    print("\n[fact_payments] Generating payments...")


    # ------------------------------------------------------
    # PASS : Generate 1,00,000 payments
    # ------------------------------------------------------
    pass_rows = []

    for order in updated_orders_map:
        order_sk         = order[0]
        customer_sk      = order[1]
        date_sk          = order[2]
        order_created_at = order[3]
        currency_code    = order[4]
        order_status     = order[5]
        total_order_amount = order[6]
        
        attempt_cnt = 0
        if order_status == "cancelled":
            attempt_cnt = 1
        else:
            attempt_cnt = random.choices([1,2,3], weights=[0.75,0.15,0.10], k=1)[0]
        
        cnt = 1
        attempt_timestamp = order_created_at
        for j in range(1, attempt_cnt+1):
            payment_attempt_id = f"PAY-{order_sk:05d}-{j}"

            payment_method = ""
            if attempt_cnt == 1:
                payment_method = random.choices (
                    PAYMENT_METHODS, weights=[0.20, 0.10, 0.25, 0.10, 0.30, 0.05], k=1
                )[0]
            else:
                payment_method = random.choices (
                    PAYMENT_METHODS, weights=[0.25, 0.20, 0.25, 0.10, 0, 0.20], k=1
                )[0]
            
            if payment_method != "cod":
                payment_timestamp = random_datetime_between(
                    attempt_timestamp,
                    attempt_timestamp + timedelta(minutes=30)
                )
            else:
                payment_timestamp = attempt_timestamp
            attempt_timestamp = payment_timestamp

            payment_date_sk = date_to_sk(payment_timestamp.date())
            
            payment_provider = random.choice(PAYMENT_PROVIDERS[payment_method])

            payment_status = ""
            if payment_method == "cod":
                if order_status == "delivered":
                    payment_status = "success"
                else:
                    payment_status = "pending"
            elif order_status == "cancelled":
                payment_status = random.choice(["failed", "cancelled"])
            else:
                if(cnt == attempt_cnt):
                    payment_status = "success"
                else:
                    payment_status = "failed"
            cnt += 1
            gateway_response_code = random.choice(GATEWAY_CODES[payment_status])
            amount = total_order_amount

            pass_rows.append((
                payment_attempt_id,
                order_sk,
                customer_sk,
                payment_date_sk,
                payment_timestamp,
                payment_method,
                payment_provider,
                payment_status,
                gateway_response_code,
                amount,
                currency_code,
                None
            ))
    bulk_insert(
        conn,
        table="dw.fact_payments",
        columns=[
            "payment_attempt_id", "order_sk", "customer_sk",
            "payment_date_sk", "payment_timestamp", "payment_method",
            "payment_provider", "payment_status", "gateway_response_code",
            "amount", "currency_code", "amount_inr"
        ],
        rows=pass_rows
    )

    print(f"  Done. Payments inserted.")
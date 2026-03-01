# etl/extract/generator/gen_orders.py
# ============================================================
# Generates and inserts fact_orders rows.
# total 1,00,000 orders
# ============================================================

import random
from datetime import date

from .config import (
    NUM_ORDERS, SCD2_CUTOFF,
    COUNTRY_CURRENCY
)
from .db import bulk_insert, fetch_all, date_to_sk, random_datetime, \
resolve_customer_at_time


def generate_orders(conn, customer_map):
    """
    Generates and inserts fact_orders rows.
    total 1,00,000 orders
    """

    print("\n[fact_orders] Generating orders...")


    # ------------------------------------------------------
    # PASS : Generate 1,00,000 orders
    # ------------------------------------------------------
    pass_rows = []

    for i in range(1, NUM_ORDERS + 1):
        order_id = f"ORD-{i:05d}"

        # Generate cration date between 2020 and present date
        order_created_at = random_datetime(date(2020, 1, 1), date(2026, 3, 1))
        order_last_updated_at = order_created_at
        date_sk = date_to_sk(order_created_at.date())

        customer = resolve_customer_at_time(customer_map, order_created_at)
        customer_sk = customer[0]

        status = []
        status_weight = []
        if order_created_at.date() < SCD2_CUTOFF:
            status = ["delivered", "cancelled"]
            status_weight = [0.85, 0.15]
        else:
            status = ["delivered", "shipped", "cancelled", "created"]
            status_weight = [0.70, 0.10, 0.15, 0.05]

        order_status = random.choices(status, weights=status_weight, k=1)[0]
        order_channel = random.choice(["web", "mobile", "marketplace"])

        currency_code = COUNTRY_CURRENCY[customer[1]]


        pass_rows.append((
            order_id,
            customer_sk,
            date_sk,
            order_created_at,
            order_last_updated_at,
            order_status,
            order_channel,
            0,
            0,
            currency_code,
            0,
            0,
        ))

    bulk_insert(
        conn,
        table="dw.fact_orders",
        columns= [
            "order_id", "customer_sk", "date_sk", "order_created_at",
            "order_last_updated_at", "order_status", "order_channel", 
            "total_order_amount", "order_discount_total", "currency_code",
            "total_order_amount_inr", "order_discount_total_inr"
        ],
        rows=pass_rows
    )

    print(f"  Done. {NUM_ORDERS} orders inserted.")
   
    # ------------------------------------------------------
    # Return all orders
    # ------------------------------------------------------
    all_orders = fetch_all(
        conn,
        """
        SELECT order_sk, customer_sk, date_sk,
            order_created_at, currency_code, order_status
        FROM dw.fact_orders
        """
    )

    return all_orders
# etl/extract/generator/gen_order_items.py
# ============================================================
# Generates and inserts fact_order_items rows.
# total ~1,20,000 order items
# ============================================================

import random
from datetime import date

from .config import (
    PRICE_RANGES,
    DISCOUNT_TIERS, DISCOUNT_WEIGHTS,
)
from .db import bulk_insert, fetch_all, execute_many, resolve_product_at_time


def generate_order_items(conn, orders_map, product_map):
    """
    Generates and inserts fact_order_items rows.
    total ~1,20,000 order items
    """

    print("\n[fact_order_items] Generating order items...")


    # ------------------------------------------------------
    # PASS 1: Generate ~1,20,000 order items
    # ------------------------------------------------------
    pass1_rows = []

    # Fetch exchange rates once
    rates = fetch_all(conn, """
        SELECT date_sk, currency_code, rate_to_inr
        FROM dw.dim_exchange_rate
    """)

    # Build a lookup dict: (date_sk, currency_code) → rate_to_inr
    rate_lookup = {(r[0], r[1]): r[2] for r in rates}

    for order in orders_map:
        order_sk         = order[0]
        customer_sk      = order[1]
        date_sk          = order[2]
        order_created_at = order[3]
        currency_code    = order[4]
        order_status     = order[5]

        item_count = random.choices([1, 2, 3, 4], weights=[0.5, 0.3, 0.1, 0.1], k=1)[0]

        for j in range(1, item_count + 1):
            order_item_id = f"ITEM-{order_sk:05d}-{j}"  # unique per order
            
            product = resolve_product_at_time(product_map, order_created_at)
            product_sk = product[0]
            category_name = product[1]

            rate = rate_lookup.get((date_sk, currency_code), 1)
            unit_price_inr = round(random.uniform(*PRICE_RANGES[category_name]), 2)
            unit_price_at_order = round(float(unit_price_inr) / float(rate), 2)
            
            quantity = 0
            if unit_price_inr < 1000:
                quantity = random.randint(1, 5)
            elif unit_price_inr >= 1000 and unit_price_inr < 10000:
                quantity = random.randint(1, 3)
            else:
                quantity = 1
            
            discount_rate = random.choices(DISCOUNT_TIERS, weights=DISCOUNT_WEIGHTS, k=1)[0]
            discount_amount = round((quantity*unit_price_at_order)*discount_rate, 2)
            line_total_amount = round((quantity*unit_price_at_order) - discount_amount, 2)

            pass1_rows.append((
                order_item_id,
                order_sk,
                product_sk,
                customer_sk,
                date_sk,
                quantity,
                unit_price_at_order,
                discount_amount,
                line_total_amount,
                None,
            ))

    bulk_insert(
        conn,
        table="dw.fact_order_items",
        columns= [
            "order_item_id", "order_sk", "product_sk", "customer_sk",
            "date_sk", "quantity", "unit_price_at_order", 
            "discount_amount", "line_total_amount", "line_total_amount_inr"
        ],
        rows=pass1_rows
    )

    print(f"  Done. ~1,20,000 order items inserted.")

    # ------------------------------------------------------
    # PASS 2: Update fact_orders with real totals
    # ------------------------------------------------------
    print("  Updating fact_orders totals...")

    order_totals = fetch_all(conn, """
        SELECT order_sk,
            SUM(line_total_amount)  AS total_order_amount,
            SUM(discount_amount)    AS order_discount_total
        FROM dw.fact_order_items
        GROUP BY order_sk
    """)

    update_rows = [
        (total, discount, order_sk)
        for order_sk, total, discount in order_totals
    ]

    execute_many(
        conn,
        """
        UPDATE dw.fact_orders
        SET total_order_amount   = %s,
            order_discount_total = %s
        WHERE order_sk = %s
        """,
        update_rows
    )
    print("  fact_orders totals updated.")

    # ------------------------------------------------------
    # Return all order items
    # ------------------------------------------------------
    all_order_items = fetch_all(
        conn,
        """
        SELECT order_item_sk, order_item_id, order_sk, product_sk, customer_sk,
            date_sk, quantity, unit_price_at_order,
            discount_amount, line_total_amount
        FROM dw.fact_order_items
        """
    )
    # ------------------------------------------------------
    # Return updated orders map with real totals
    # ------------------------------------------------------
    updated_orders = fetch_all(
        conn,
        """
        SELECT order_sk, customer_sk, date_sk,
            order_created_at, currency_code, order_status,
            total_order_amount
        FROM dw.fact_orders
        """
    )
    return all_order_items, updated_orders

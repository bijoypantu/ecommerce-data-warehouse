# etl/extract/generator/gen_refunds.py
# ============================================================
# Generates and inserts fact_refunds rows.
# total ~1,10,000 refunds
# ============================================================

import random
from datetime import date, timedelta
from collections import defaultdict

from .config import (
    REFUND_RATE, REFUND_REASONS
)
from .db import bulk_insert, date_to_sk, random_datetime_between


def generate_refunds(conn, delivered_shipments, order_items_map):
    """
    Generates and inserts fact_refunds rows.
    total ~10,000 refunds
    """

    print("\n[fact_refunds] Generating refunds...")

    # ------------------------------------------------------
    # PASS : Generate 10,000 refunds
    # ------------------------------------------------------
    pass_rows = []
    
    # Group order_items_map by order_sk
    items_by_order = defaultdict(list)
    for item in order_items_map:
        items_by_order[item[2]].append(item)  # item[2] is order_sk
    
    # Group delivered_shipments by order_sk
    shipments_by_order = defaultdict(list)
    for shipment in delivered_shipments:
        shipments_by_order[shipment[0]].append(shipment)  # shipment[0] is order_sk
    
    # Pick 10% of unique delivered order SK's
    unique_order_sks = list({row[0] for row in delivered_shipments})
    num_refunds = int(len(unique_order_sks) * REFUND_RATE)
    orders_to_refund = random.sample(unique_order_sks, num_refunds)

    for order_sk in orders_to_refund:
        shipment_rows = shipments_by_order[order_sk]
        delivered_at  = shipment_rows[0][4]   # delivered_at
        currency_code = shipment_rows[0][5]   # currency_code
        customer_sk   = shipment_rows[0][2]   # customer_sk

        order_items = items_by_order[order_sk]

        refund_type = random.choices(
            ["full", "partial_order", "partial_item"],
            weights=[0.50, 0.30, 0.20], k=1
        )[0]

        if refund_type == "full":
            items_to_refund = order_items
        elif refund_type == "partial_order":
            count = random.randint(1, len(order_items))
            items_to_refund = random.sample(order_items, count)
        else:
            items_to_refund = [random.choice(order_items)]

        for items in items_to_refund:
            order_item_sk = items[0]
            quantity = items[6]
            unit_price_at_order = items[7]

            refund_id = f"REF-{order_sk:05d}-{order_item_sk}"
            initiated_at = random_datetime_between(
                delivered_at, delivered_at + timedelta(days=3))
            refund_date_sk = date_to_sk(initiated_at.date())

            if delivered_at.date() < date(2026, 2, 1):
                # older deliveries — only processed or rejected
                refund_status = random.choices(
                    ["processed", "rejected"],
                    weights=[0.80, 0.20], k=1
                )[0]
            else:
                # recent deliveries — any status
                refund_status = random.choices(
                    ["processed", "approved", "initiated", "rejected"],
                    weights=[0.60, 0.15, 0.15, 0.10], k=1
                )[0]

            processed_at = None
            if refund_status == "processed":
                processed_at = random_datetime_between(
                    initiated_at, initiated_at + timedelta(days=7))
            
            refund_quantity = 0
            if refund_type == "partial_item":
                if quantity > 1:
                    refund_quantity = random.randint(1, quantity - 1)
                else:
                    refund_quantity = 1
            else:
                refund_quantity = quantity
            
            refund_amount = refund_quantity * unit_price_at_order
            refund_reason = random.choice(REFUND_REASONS)
            refund_amount_inr = None

            pass_rows.append((
                refund_id,
                order_sk,
                order_item_sk,
                customer_sk,
                refund_date_sk,
                initiated_at,
                processed_at,
                refund_quantity,
                refund_amount,
                refund_reason,
                refund_status,
                currency_code,
                refund_amount_inr
            ))

    bulk_insert(
        conn,
        table="dw.fact_refunds",
        columns=[
            "refund_id", "order_sk", "order_item_sk", "customer_sk",
            "refund_date_sk", "initiated_at", "processed_at",
            "refund_quantity", "refund_amount", "refund_reason",
            "refund_status", "currency_code", "refund_amount_inr"
        ],
        rows=pass_rows
    )

    print(f"  Done. refunds inserted.")
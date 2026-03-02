# etl/extract/generator/gen_shipments.py
# ============================================================
# Generates and inserts fact_shipments rows.
# total ~1,10,000 shipments
# ============================================================

import random
from datetime import timedelta
from collections import defaultdict
from uuid import uuid4

from .config import (
    CARRIERS
)
from .db import bulk_insert, date_to_sk, random_datetime_between, \
fetch_all, execute_many


def generate_shipments(conn, updated_orders_map, order_items_map):
    """
    Generates and inserts fact_shipments rows.
    total 1,10,000 shipments
    """

    print("\n[fact_shipments] Generating shipments...")


    # ------------------------------------------------------
    # PASS : Generate 1,00,000 shipments
    # ------------------------------------------------------
    pass_rows = []

    items_by_order = defaultdict(list)
    for item in order_items_map:
        items_by_order[item[2]].append(item)  # item[2] is order_sk

    for order in updated_orders_map:
        order_sk         = order[0]
        customer_sk      = order[1]
        order_created_at = order[3]
        order_status     = order[5]
        
        if order_status in ("cancelled", "created"):
            continue

        order_items = items_by_order[order_sk]
        item_index = 1
        for item in order_items:
            order_item_sk   = item[0]
            quantity        = item[5]
            shipment_id = f"SHIP-{order_sk:05d}-{item_index}"
            item_index += 1

            shipped_at = random_datetime_between(
                order_created_at, order_created_at + timedelta(days=3))
            shipment_date_sk = date_to_sk(shipped_at.date())
            
            shipment_status = ""
            if(order_status == "delivered"):
                delivered_at = random_datetime_between(
                    shipped_at, shipped_at + timedelta(days=7))
                delivery_date_sk = date_to_sk(delivered_at.date())
                shipment_status = "delivered"
            else:
                delivered_at = None
                delivery_date_sk = None
                shipment_status = "shipped"
            
            carrier = random.choice(CARRIERS)
            tracking_id = f"{uuid4().hex}"
            shipped_quantity = quantity

            pass_rows.append((
                shipment_id,
                order_sk,
                order_item_sk,
                customer_sk,
                shipment_date_sk,
                delivery_date_sk,
                shipped_at,
                delivered_at,
                shipment_status,
                carrier,
                tracking_id,
                shipped_quantity
            ))

    bulk_insert(
        conn,
        table="dw.fact_shipments",
        columns=[
            "shipment_id", "order_sk", "order_item_sk", "customer_sk",
            "shipment_date_sk", "delivery_date_sk", "shipped_at",
            "delivered_at", "shipment_status", "carrier",
            "tracking_id", "shipped_quantity"
        ],
        rows=pass_rows
    )

    print(f"  Done. shipments inserted.")

    # Update fact_orders.order_last_updated_at from shipment timestamps
    print("  Updating fact_orders last updated timestamps...")

    shipment_updates = fetch_all (conn, """
        SELECT order_sk,
            MAX(CASE WHEN shipment_status = 'delivered' THEN delivered_at
                        ELSE shipped_at END) AS last_event
        FROM dw.fact_shipments
        GROUP BY order_sk
    """)

    update_rows = [(ts, sk) for sk, ts in shipment_updates]

    execute_many (
        conn,
        """
        UPDATE dw.fact_orders
        SET order_last_updated_at = %s
        WHERE order_sk = %s
        """,
        update_rows
    )
    print("  fact_orders timestamps updated.")

    # Return delivered shipments for gen_refunds.py
    delivered_shipments = fetch_all(conn, """
        SELECT fs.order_sk, fs.order_item_sk, fs.customer_sk,
            fs.shipment_date_sk, fs.delivered_at,
            fo.currency_code
        FROM dw.fact_shipments fs
        JOIN dw.fact_orders fo ON fo.order_sk = fs.order_sk
        WHERE fs.shipment_status = 'delivered'
    """)
    return delivered_shipments
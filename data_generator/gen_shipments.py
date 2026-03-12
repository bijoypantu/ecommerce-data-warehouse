# data_generator/gen_shipments.py
# ============================================================
# Generates fact_shipments DataFrame.
# Phase 1 — queries warehouse for processing orders → ships 80%
# Phase 2 — queries warehouse for shipped shipments → delivers 80%
#
# Returns:
#   shipments_df           — new shipment records for today
#   order_events_df        — order state update events (shipped + delivered)
# ============================================================

import random
import pandas as pd
from datetime import datetime, timezone, timedelta
from uuid import uuid4
from collections import defaultdict

from .config import CARRIERS
from .db import random_datetime_between


def generate_shipments(conn, generation_date):

    print("\n[fact_shipments] Generating shipments...")

    gen_dt      = datetime.combine(generation_date, datetime.min.time()).replace(tzinfo=timezone.utc)
    ingested_at = datetime.now(timezone.utc).isoformat()

    rows           = []
    order_events   = []

    # ----------------------------------------------------------
    # PHASE 1 — Ship 80% of processing orders
    # ----------------------------------------------------------
    sql = """
            SELECT
                fo.order_id,
                fo.order_created_at,
                (SELECT customer_id FROM dw.dim_customer dc WHERE dc.customer_sk = fo.customer_sk) as customer_id,
                fo.order_channel,
                fo.total_order_amount,
                fo.order_discount_total,
                fo.currency_code,
                foi.order_item_id,
                foi.quantity,
                foi.unit_price_at_order
            FROM dw.fact_orders fo
            JOIN dw.fact_order_items foi ON fo.order_sk = foi.order_sk
            WHERE fo.order_status = 'processing'
        """
    with conn.cursor() as cur:
        cur.execute(sql)
        processing_rows = cur.fetchall()

    # Group items by order_id
    items_by_order = defaultdict(list)
    order_meta     = {}

    for row in processing_rows:
        (order_id, order_created_at, customer_id, order_channel,
         total_order_amount, order_discount_total, currency_code,
         order_item_id, quantity, unit_price_at_order) = row

        items_by_order[order_id].append({
            "order_item_id":       order_item_id,
            "quantity":            quantity,
            "unit_price_at_order": unit_price_at_order,
        })
        order_meta[order_id] = {
            "order_created_at":     order_created_at,
            "customer_id":          customer_id,
            "order_channel":        order_channel,
            "total_order_amount":   total_order_amount,
            "order_discount_total": order_discount_total,
            "currency_code":        currency_code,
        }

    # Randomly select 80% of orders to ship
    all_order_ids  = list(items_by_order.keys())
    num_to_ship    = int(len(all_order_ids) * 0.80)
    orders_to_ship = random.sample(all_order_ids, num_to_ship)

    for order_id in orders_to_ship:
        meta       = order_meta[order_id]
        shipped_at = random_datetime_between(
            gen_dt,
            gen_dt + timedelta(hours=23)
        )

        for idx, item in enumerate(items_by_order[order_id], 1):
            rows.append({
                "shipment_id":       f"SHIP-{order_id}-{idx}",
                "order_id":          order_id,
                "order_item_id":     item["order_item_id"],
                "customer_id":       meta["customer_id"],
                "shipped_at":        shipped_at,
                "delivered_at":      None,
                "shipment_status":   "shipped",
                "carrier":           random.choice(CARRIERS),
                "tracking_id":       uuid4().hex,
                "shipped_quantity":  item["quantity"],
                "event_type":        "shipment_created",
                "ingested_at":       ingested_at,
            })

        order_events.append({
            "order_id":                 order_id,
            "customer_id":              meta["customer_id"],
            "order_created_at":         meta["order_created_at"],
            "order_last_updated_at":    shipped_at,
            "order_status":             "shipped",
            "order_channel":            meta["order_channel"],
            "total_order_amount":       meta["total_order_amount"],
            "order_discount_total":     meta["order_discount_total"],
            "currency_code":            meta["currency_code"],
            "total_order_amount_inr":   None,
            "order_discount_total_inr": None,
            "event_type":               "order_shipped",
            "ingested_at":              ingested_at,
        })

    # ----------------------------------------------------------
    # PHASE 2 — Deliver 80% of already shipped shipments
    # ----------------------------------------------------------
    sql = """
            SELECT
                fs.shipment_id,
                foi.order_item_id,
                fs.shipped_at,
                fo.order_id,
                fo.order_created_at,
                (SELECT customer_id FROM dw.dim_customer dc WHERE dc.customer_sk = fo.customer_sk) as customer_id,
                fo.order_channel,
                fo.total_order_amount,
                fo.order_discount_total,
                fo.currency_code,
                foi.quantity,
                foi.unit_price_at_order
            FROM dw.fact_shipments fs
            JOIN dw.fact_order_items foi ON fs.order_item_sk = foi.order_item_sk
            JOIN dw.fact_orders fo ON foi.order_sk = fo.order_sk
            WHERE fs.shipment_status = 'shipped'
        """
    with conn.cursor() as cur:
        cur.execute(sql)
        shipped_rows = cur.fetchall()

    num_to_deliver  = int(len(shipped_rows) * 0.80)
    rows_to_deliver = random.sample(shipped_rows, num_to_deliver)

    seen_order_ids = set()
    for row in rows_to_deliver:
        (shipment_id, order_item_id, shipped_at, order_id,
         order_created_at, customer_id, order_channel,
         total_order_amount, order_discount_total, currency_code,
         quantity, unit_price_at_order) = row

        delivered_at = random_datetime_between(
            gen_dt,
            gen_dt + timedelta(hours=23)
        )

        rows.append({
            "shipment_id":       shipment_id,
            "order_id":          order_id,
            "order_item_id":     order_item_id,
            "customer_id":       customer_id,
            "shipped_at":        shipped_at,
            "delivered_at":      delivered_at,
            "shipment_status":   "delivered",
            "carrier":           None,
            "tracking_id":       None,
            "shipped_quantity":  quantity,
            "event_type":        "shipment_delivered",
            "ingested_at":       ingested_at,
        })

        if order_id not in seen_order_ids:
            seen_order_ids.add(order_id)
            order_events.append({
                "order_id":                 order_id,
                "customer_id":              customer_id,
                "order_created_at":         order_created_at,
                "order_last_updated_at":    delivered_at,
                "order_status":             "delivered",
                "order_channel":            order_channel,
                "total_order_amount":       total_order_amount,
                "order_discount_total":     order_discount_total,
                "currency_code":            currency_code,
                "total_order_amount_inr":   None,
                "order_discount_total_inr": None,
                "event_type":               "order_delivered",
                "ingested_at":              ingested_at,
            })

    shipments_df           = pd.DataFrame(rows)
    order_events_df        = pd.DataFrame(order_events)

    print(f"  Done. {len(shipments_df)} shipment records generated.")
    print(f"  {num_to_ship} orders shipped, {num_to_deliver} shipments delivered.")

    return shipments_df, order_events_df
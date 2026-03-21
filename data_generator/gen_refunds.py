# data_generator/gen_refunds.py
# ============================================================
# Generates fact_refunds DataFrame.
# Only delivered orders are eligible for refunds.
# Returns refunds_df for Bronze layer writing.
# ============================================================

import random
import pandas as pd
from datetime import datetime, timezone, timedelta
from collections import defaultdict

from .config import REFUND_REASONS
from .db import random_datetime_between

def generate_refunds(conn, generation_date):
    print("\n[fact_refunds] Generating refunds...")

    gen_dt      = datetime.combine(generation_date, datetime.min.time()).replace(tzinfo=timezone.utc)
    ingested_at = datetime.now(timezone.utc).isoformat()

    # ----------------------------------------------------------
    # PHASE 1 — refund 10% of delivered orders
    # ----------------------------------------------------------
    sql = """
        SELECT
            fo.order_id,
            fo.order_created_at,
            (SELECT customer_id FROM dw.dim_customer dc WHERE dc.customer_sk = fo.customer_sk) as customer_id,
            fo.currency_code,
            foi.order_item_id,
            foi.quantity,
            foi.unit_price_at_order,
            foi.discount_amount
        FROM dw.fact_orders fo
        JOIN dw.fact_order_items foi ON fo.order_sk = foi.order_sk
        WHERE fo.order_status = 'delivered'
            AND fo.order_last_updated_at >= %(gen_date)s::date - INTERVAL '3 days'
            AND fo.order_last_updated_at < %(gen_date)s::date + INTERVAL '1 day'
            AND fo.order_id NOT IN (SELECT order_id FROM dw.fact_refunds fr JOIN dw.fact_orders fo ON fr.order_sk = fo.order_sk)
    """

    with conn.cursor() as cur:
        cur.execute(sql, {"gen_date": generation_date.isoformat()})
        delivered_rows = cur.fetchall()
    
    # Group items by order_id
    items_by_order = defaultdict(list)
    order_meta     = {}

    for row in delivered_rows:
        (order_id, order_created_at, customer_id, currency_code,
         order_item_id, quantity, unit_price_at_order,
         discount_amount) = row
        
        items_by_order[order_id].append({
            "order_item_id":       order_item_id,
            "quantity":            quantity,
            "unit_price_at_order": unit_price_at_order,
            "discount_amount":    discount_amount,
        })
        order_meta[order_id] = {
            "order_created_at":     order_created_at,
            "customer_id":          customer_id,
            "currency_code":        currency_code,
        }

    # Randomly select 10% of orders to refund
    all_order_ids  = list(items_by_order.keys())
    num_refunds    = int(len(all_order_ids) * 0.08)
    orders_to_refund = random.sample(all_order_ids, num_refunds)

    rows = []

    for order_id in orders_to_refund:
        meta       = order_meta[order_id]
        order_items = items_by_order[order_id]

        initiated_at = random_datetime_between(
            gen_dt,
            gen_dt + timedelta(hours=23)
        )


        # Decide refund type
        refund_type = random.choices(
            ["full", "partial_order", "partial_item"],
            weights=[0.50, 0.30, 0.20], k=1
        )[0]

        if refund_type == "full":
            items_to_refund = order_items
        elif refund_type == "partial_order":
            count           = random.randint(1, len(order_items))
            items_to_refund = random.sample(order_items, count)
        else:
            items_to_refund = [random.choice(order_items)]

        for idx, item in enumerate(items_to_refund, 1):
            quantity = item["quantity"]
            unit_price_at_order = item["unit_price_at_order"]
            discount_amount = item["discount_amount"]

            refund_id    = f"REF-{order_id}-{idx}"

            # Refund quantity
            if refund_type == "partial_item":
                if quantity > 1:
                    refund_quantity = random.randint(1, quantity - 1)
                else:
                    refund_quantity = 1
            else:
                refund_quantity = quantity

            discount_per_unit = discount_amount / quantity if quantity > 0 else 0
            refund_amount = round((refund_quantity * unit_price_at_order) - (refund_quantity * discount_per_unit), 2)

            refund_reason = random.choice(REFUND_REASONS)

            rows.append({
                "refund_id":          refund_id,
                "order_id":           order_id,
                "order_item_id":      item["order_item_id"],
                "customer_id":        meta["customer_id"],
                "initiated_at":       initiated_at,
                "processed_at":       None,
                "refund_quantity":    refund_quantity,
                "refund_amount":      refund_amount,
                "refund_reason":      refund_reason,
                "refund_status":      "initiated",
                "currency_code":      meta["currency_code"],
                "refund_amount_inr":  None,
                "event_type":         "refund_initiated",
                "ingested_at":        ingested_at,
            })
        
    # ----------------------------------------------------------
    # PHASE 2 — Choose 80% random refund requests
    # 1. Reject 30% of those requests
    # 2. Accept 70% of those requests
    # ----------------------------------------------------------
    sql = """
        SELECT
            fr.refund_id,
            fo.order_id,
            foi.order_item_id,
            (SELECT customer_id FROM dw.dim_customer dc WHERE dc.customer_sk = fo.customer_sk) as customer_id,
            fr.initiated_at,
            fr.refund_quantity,
            fr.refund_amount,
            fr.refund_reason,
            fr.currency_code
        FROM dw.fact_refunds fr
        JOIN dw.fact_orders fo ON fo.order_sk = fr.order_sk
        JOIN dw.fact_order_items foi ON fr.order_item_sk = foi.order_item_sk
        WHERE fr.refund_status = 'initiated'
    """

    with conn.cursor() as cur:
        cur.execute(sql)
        refund_rows = cur.fetchall()
    
    num_to_refund = int(len(refund_rows) * 0.80)
    rows_to_refund = random.sample(refund_rows, num_to_refund)

    for row in rows_to_refund:
        (refund_id, order_id, order_item_id, customer_id, initiated_at,
        refund_quantity, refund_amount, refund_reason, currency_code) = row

        processed_at = random_datetime_between(
            initiated_at,
            initiated_at + timedelta(days=3)
        )

        status = random.choices(
            ["processed", "rejected"],
            weights=[0.70, 0.30], k=1
        )[0]

        rows.append({
                "refund_id":          refund_id,
                "order_id":           order_id,
                "order_item_id":      order_item_id,
                "customer_id":        customer_id,
                "initiated_at":       initiated_at,
                "processed_at":       processed_at if status == "processed" else None,
                "refund_quantity":    refund_quantity,
                "refund_amount":      refund_amount,
                "refund_reason":      refund_reason,
                "refund_status":      status,
                "currency_code":      currency_code,
                "refund_amount_inr":  None,
                "event_type":         "refund_processed",
                "ingested_at":        ingested_at,
            })
    
    refunds_df = pd.DataFrame(rows)

    print(f"  Done. {len(refunds_df)} refund records generated.")
    print(f"  {num_refunds} refunds initiated, {num_to_refund} refunds processed.")

    return refunds_df
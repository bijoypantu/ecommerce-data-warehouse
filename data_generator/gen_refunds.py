# data_generator/gen_refunds.py
# ============================================================
# Generates fact_refunds DataFrame.
# Only delivered orders are eligible for refunds.
# Returns refunds_df for Bronze layer writing.
# ============================================================

import random
import pandas as pd
from datetime import date, datetime, timezone, timedelta
from collections import defaultdict

from .config import REFUND_RATE, REFUND_REASONS
from .db import random_datetime_between


def generate_refunds(delivered_shipments_df, items_df):
    """
    Generates fact_refunds DataFrame.
    10% of delivered orders get refunded.
    Returns refunds_df.
    """

    print("\n[fact_refunds] Generating refunds...")

    # ----------------------------------------------------------
    # Group items by order_id for quantity and price lookup
    # ----------------------------------------------------------
    items_by_order = defaultdict(list)
    for _, item in items_df.iterrows():
        items_by_order[item["order_id"]].append(item)

    # ----------------------------------------------------------
    # Group delivered shipments by order_id
    # ----------------------------------------------------------
    shipments_by_order = defaultdict(list)
    for _, shipment in delivered_shipments_df.iterrows():
        shipments_by_order[shipment["order_id"]].append(shipment)

    # ----------------------------------------------------------
    # Pick 10% of unique delivered orders for refund
    # ----------------------------------------------------------
    unique_order_ids = list(shipments_by_order.keys())
    num_refunds      = int(len(unique_order_ids) * REFUND_RATE)
    orders_to_refund = random.sample(unique_order_ids, num_refunds)

    rows = []

    for order_id in orders_to_refund:
        shipment_rows = shipments_by_order[order_id]
        delivered_at  = shipment_rows[0]["delivered_at"]
        currency_code = shipment_rows[0]["currency_code"]
        customer_id   = shipment_rows[0]["customer_id"]

        order_items = items_by_order[order_id]

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
            order_item_id       = item["order_item_id"]
            quantity            = item["quantity"]
            unit_price_at_order = item["unit_price_at_order"]

            refund_id    = f"REF-{order_id}-{idx}"
            initiated_at = random_datetime_between(
                delivered_at,
                delivered_at + timedelta(days=3)
            )

            # Refund status based on delivery date
            if delivered_at.date() < date(2026, 2, 1):
                refund_status = random.choices(
                    ["processed", "rejected"],
                    weights=[0.80, 0.20], k=1
                )[0]
            else:
                refund_status = random.choices(
                    ["processed", "approved", "initiated", "rejected"],
                    weights=[0.60, 0.15, 0.15, 0.10], k=1
                )[0]

            processed_at = None
            if refund_status == "processed":
                processed_at = random_datetime_between(
                    initiated_at,
                    initiated_at + timedelta(days=7)
                )

            # Refund quantity
            if refund_type == "partial_item":
                if quantity > 1:
                    refund_quantity = random.randint(1, quantity - 1)
                else:
                    refund_quantity = 1
            else:
                refund_quantity = quantity

            refund_amount = round(refund_quantity * unit_price_at_order, 2)
            refund_reason = random.choice(REFUND_REASONS)

            rows.append({
                "refund_id":          refund_id,
                "order_id":           order_id,
                "order_item_id":      order_item_id,
                "customer_id":        customer_id,
                "initiated_at":       initiated_at,
                "processed_at":       processed_at,
                "refund_quantity":    refund_quantity,
                "refund_amount":      refund_amount,
                "refund_reason":      refund_reason,
                "refund_status":      refund_status,
                "currency_code":      currency_code,
                "refund_amount_inr":  None,
                "event_type":         "refund_initiated",
                "ingested_at":        datetime.now(timezone.utc).isoformat(),
            })

    refunds_df = pd.DataFrame(rows)

    print(f"  Done. {len(refunds_df)} refund records generated.")
    return refunds_df
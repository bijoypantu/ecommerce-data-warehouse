# data_generator/gen_shipments.py
# ============================================================
# Generates fact_shipments DataFrame.
# Appends order_shipped and order_delivered events to orders_df.
# Returns shipments_df, orders_df, delivered_shipments_df.
# ============================================================

import random
import pandas as pd
from datetime import datetime, timezone, timedelta
from uuid import uuid4
from collections import defaultdict

from .config import CARRIERS
from .db import random_datetime_between


def generate_shipments(orders_df, items_df):
    """
    Generates fact_shipments DataFrame.
    Appends order_shipped and order_delivered events to orders_df.
    Returns shipments_df, orders_df, delivered_shipments_df.
    """

    print("\n[fact_shipments] Generating shipments...")

    # ----------------------------------------------------------
    # Get only processing orders — eligible for shipment
    # ----------------------------------------------------------
    processing_orders = orders_df[
        orders_df["event_type"] == "order_processing"
    ].copy()

    # ----------------------------------------------------------
    # Group items by order_id
    # ----------------------------------------------------------
    items_by_order = defaultdict(list)
    for _, item in items_df.iterrows():
        items_by_order[item["order_id"]].append(item)

    # ----------------------------------------------------------
    # Randomly select 80% of processing orders to ship
    # Split into delivered (70/80) and shipped (10/80)
    # ----------------------------------------------------------
    num_to_ship      = int(len(processing_orders) * 0.80)
    orders_to_ship   = processing_orders.sample(num_to_ship)

    num_delivered    = int(len(orders_to_ship) * (70/80))
    delivered_orders = orders_to_ship.iloc[:num_delivered]
    shipped_orders   = orders_to_ship.iloc[num_delivered:]

    rows             = []
    order_events     = []
    delivered_rows   = []

    # ----------------------------------------------------------
    # Process delivered orders
    # ----------------------------------------------------------
    for _, order in delivered_orders.iterrows():
        order_id         = order["order_id"]
        customer_id      = order["customer_id"]
        order_created_at = order["order_created_at"]
        currency_code    = order["currency_code"]

        shipped_at = random_datetime_between(
            order_created_at,
            order_created_at + timedelta(days=3)
        )
        delivered_at = random_datetime_between(
            shipped_at,
            shipped_at + timedelta(days=7)
        )

        order_items = items_by_order[order_id]
        for idx, item in enumerate(order_items, 1):
            shipment_id = f"SHIP-{order_id}-{idx}"

            rows.append({
                "shipment_id":       shipment_id,
                "order_id":          order_id,
                "order_item_id":     item["order_item_id"],
                "customer_id":       customer_id,
                "shipped_at":        shipped_at,
                "delivered_at":      delivered_at,
                "shipment_status":   "delivered",
                "carrier":           random.choice(CARRIERS),
                "tracking_id":       uuid4().hex,
                "shipped_quantity":  item["quantity"],
                "event_type":        "shipment_created",
                "ingested_at":       datetime.now(timezone.utc).isoformat(),
            })

            delivered_rows.append({
                "order_id":           order_id,
                "order_item_id":      item["order_item_id"],
                "customer_id":        customer_id,
                "delivered_at":       delivered_at,
                "currency_code":      currency_code,
                "quantity":           item["quantity"],
                "unit_price_at_order": item["unit_price_at_order"],
            })

        # Append order_delivered event
        order_events.append({
            "order_id":              order_id,
            "customer_id":           customer_id,
            "order_created_at":      order_created_at,
            "order_last_updated_at": delivered_at,
            "order_status":          "delivered",
            "order_channel":         order["order_channel"],
            "total_order_amount":    order["total_order_amount"],
            "order_discount_total":  order["order_discount_total"],
            "currency_code":         currency_code,
            "total_order_amount_inr":   None,
            "order_discount_total_inr": None,
            "event_type":            "order_delivered",
            "ingested_at":           datetime.now(timezone.utc).isoformat(),
        })

    # ----------------------------------------------------------
    # Process shipped orders (in transit)
    # ----------------------------------------------------------
    for _, order in shipped_orders.iterrows():
        order_id         = order["order_id"]
        customer_id      = order["customer_id"]
        order_created_at = order["order_created_at"]
        currency_code    = order["currency_code"]

        shipped_at = random_datetime_between(
            order_created_at,
            order_created_at + timedelta(days=3)
        )

        order_items = items_by_order[order_id]
        for idx, item in enumerate(order_items, 1):
            shipment_id = f"SHIP-{order_id}-{idx}"

            rows.append({
                "shipment_id":       shipment_id,
                "order_id":          order_id,
                "order_item_id":     item["order_item_id"],
                "customer_id":       customer_id,
                "shipped_at":        shipped_at,
                "delivered_at":      None,
                "shipment_status":   "shipped",
                "carrier":           random.choice(CARRIERS),
                "tracking_id":       uuid4().hex,
                "shipped_quantity":  item["quantity"],
                "event_type":        "shipment_created",
                "ingested_at":       datetime.now(timezone.utc).isoformat(),
            })

        # Append order_shipped event
        order_events.append({
            "order_id":              order_id,
            "customer_id":           customer_id,
            "order_created_at":      order_created_at,
            "order_last_updated_at": shipped_at,
            "order_status":          "shipped",
            "order_channel":         order["order_channel"],
            "total_order_amount":    order["total_order_amount"],
            "order_discount_total":  order["order_discount_total"],
            "currency_code":         currency_code,
            "total_order_amount_inr":   None,
            "order_discount_total_inr": None,
            "event_type":            "order_shipped",
            "ingested_at":           datetime.now(timezone.utc).isoformat(),
        })

    shipments_df          = pd.DataFrame(rows)
    delivered_shipments_df = pd.DataFrame(delivered_rows)
    orders_df             = pd.concat(
        [orders_df, pd.DataFrame(order_events)],
        ignore_index=True
    )

    print(f"  Done. {len(shipments_df)} shipment records generated.")
    print(f"  {len(delivered_orders)} orders delivered, {len(shipped_orders)} orders in transit.")
    return shipments_df, orders_df, delivered_shipments_df
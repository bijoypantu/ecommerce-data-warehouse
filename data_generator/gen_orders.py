# etl/extract/generator/gen_orders.py
# ============================================================
# Generates fact_orders DataFrame.
# All orders start as "order_created" events.
# Status progression happens in subsequent generators.
# Returns orders_df for Bronze layer writing.
# ============================================================

import random
import pandas as pd
from datetime import date, datetime, timezone

from .config import (
    NUM_ORDERS, COUNTRY_CURRENCY
)
from .db import random_datetime, resolve_customer_at_time

def generate_orders(customer_versions):
    """
    Generates all orders as "order_created" events.
    No status distribution — that happens in payments and shipments.
    Returns orders_df.
    """

    print("\n[fact_orders] Generating orders...")

    rows = []

    for i in range(1, NUM_ORDERS + 1):
        order_id = f"ORD-{i:05d}"

        # Generate order creation datetime
        year = random.choices(
            [2021, 2022, 2023, 2024, 2025, 2026],
            weights=[0.05, 0.10, 0.15, 0.20, 0.35, 0.15],
            k=1
        )[0]

        if year == 2026:
            order_created_at = random_datetime(date(2026, 1, 1), date(2026, 3, 1))
        else:
            order_created_at = random_datetime(date(year, 1, 1), date(year, 12, 31))

        # Resolve customer active at order time
        customer_id, country = resolve_customer_at_time(
            customer_versions, order_created_at
        )

        # Handle edge case where no customer found
        if customer_id is None:
            continue

        # Derive currency from customer's country
        currency_code = COUNTRY_CURRENCY[country]

        # Order channel
        order_channel = random.choice(["web", "mobile", "marketplace"])

        rows.append({
            "order_id":            order_id,
            "customer_id":         customer_id,
            "order_created_at":    order_created_at,
            "order_last_updated_at": order_created_at,  # placeholder
            "order_status":        "created",
            "order_channel":       order_channel,
            "total_order_amount":  0,           # updated by gen_order_items
            "order_discount_total": 0,          # updated by gen_order_items
            "currency_code":       currency_code,
            "total_order_amount_inr":   None,   # calculated in ETL
            "order_discount_total_inr": None,   # calculated in ETL
            "event_type":          "order_created",
            "ingested_at":         datetime.now(timezone.utc).isoformat(),
        })

    df = pd.DataFrame(rows)

    print(f"  Done. {len(df)} orders generated.")
    return df
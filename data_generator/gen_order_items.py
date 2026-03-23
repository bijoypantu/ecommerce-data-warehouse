# etl/extract/generator/gen_order_items.py
# ============================================================
# Generates fact_order_items DataFrame.
# Returns order_items_df, orders_df for Bronze layer writing.
# ============================================================

import random
import pandas as pd
from datetime import datetime, timezone

from .config import (
    PRICE_RANGES,
    DISCOUNT_TIERS, DISCOUNT_WEIGHTS,
)
from .db import date_to_sk


def generate_order_items(conn, orders_df, rate_lookup):
   
    print("\n[fact_order_items] Generating order items...")


    # ------------------------------------------------------
    # PASS 1: Generate order items for present day orders
    # ------------------------------------------------------
    rows = []

    with conn.cursor() as cur:
        cur.execute("""
            SELECT dp.product_id, dc.category_name
            FROM dw.dim_product dp
            JOIN dw.dim_category dc ON dp.category_sk = dc.category_sk
            WHERE dp.is_current = true and product_status = 'active'
        """)
        active_products = cur.fetchall()

    for _, order in orders_df.iterrows():
        order_id         = order["order_id"]
        customer_id      = order["customer_id"]
        order_created_at = order["order_created_at"]
        currency_code    = order["currency_code"]

        item_count = random.choices([1, 2, 3, 4], weights=[0.4, 0.3, 0.2, 0.1], k=1)[0]
        for j in range(1, item_count + 1):
            order_item_id = f"ITEM-{order_id}-{j}"  # unique per order
            
            product_id, category_name = random.choice(active_products)

            date_sk = date_to_sk(order_created_at.date())
            rate = rate_lookup.get((date_sk, currency_code), 1)

            min_price, max_price = PRICE_RANGES[category_name]
            mode_price = min_price + (max_price - min_price) * 0.2  # mode at 20% of range
            unit_price_inr = round(random.triangular(min_price, max_price, mode_price), 2)
            
            unit_price_at_order = round(float(unit_price_inr) / float(rate), 2)

            quantity = 0
            if unit_price_inr < 1000:
                quantity = random.randint(1, 5)
            elif unit_price_inr >= 1000 and unit_price_inr < 10000:
                quantity = random.randint(1, 3)
            else:
                quantity = 1
            
            discount_rate = random.choices(DISCOUNT_TIERS, weights=DISCOUNT_WEIGHTS, k=1)[0]
            total_amount = quantity*unit_price_at_order
            discount_amount = round(total_amount*discount_rate, 2)
            line_total_amount = round(total_amount - discount_amount, 2)


            rows.append({
                "order_item_id":        order_item_id,
                "order_id":             order_id,
                "product_id":           product_id,
                "customer_id":          customer_id,
                "order_created_at":     order_created_at,
                "quantity":             quantity,
                "unit_price_at_order":  unit_price_at_order,
                "total_amount":         total_amount,
                "discount_amount":      discount_amount,
                "line_total_amount":    line_total_amount,
                "event_type":          "order_item_created",
                "ingested_at":         datetime.now(timezone.utc).isoformat(),
            })

    df = pd.DataFrame(rows)

    print(f"  Done. {len(df)} order items generated.")

    # ------------------------------------------------------
    # PASS 2: Append order_totals_updated events to orders_df
    # ------------------------------------------------------
    print("  Updating order totals...")

    # Calculate totals per order
    totals = (
        df.groupby("order_id")
        .agg(
            total_order_amount=("total_amount", "sum"),
            order_discount_total=("discount_amount", "sum")
        )
        .reset_index()
    )

    # Build update event rows
    update_rows = []
    for _, row in totals.iterrows():
        # Get original order row for other fields
        original = orders_df[orders_df["order_id"] == row["order_id"]].iloc[0]
        update_rows.append({
            "order_id":              row["order_id"],
            "customer_id":           original["customer_id"],
            "order_created_at":      original["order_created_at"],
            "order_last_updated_at": original["order_created_at"],
            "order_status":          "created",
            "order_channel":         original["order_channel"],
            "total_order_amount":    row["total_order_amount"],
            "order_discount_total":  row["order_discount_total"],
            "currency_code":         original["currency_code"],
            "total_order_amount_inr":   None,
            "order_discount_total_inr": None,
            "event_type":            "order_totals_updated",
            "ingested_at":           datetime.now(timezone.utc).isoformat(),
        })

    orders_df = pd.concat(
        [orders_df, pd.DataFrame(update_rows)],
        ignore_index=True
    )

    print(f"  Done. {len(totals)} order total events appended.")
    return df, orders_df
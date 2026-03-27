# etl/extract/generator/gen_orders.py
# ============================================================
# Generates fact_orders DataFrame.
# All orders start as "order_created" events.
# Status progression happens in subsequent generators.
# Returns orders_df for Bronze layer writing.
# ============================================================

import random
import pandas as pd
from datetime import date, datetime, timezone, timedelta

from etl.utils.logger import get_logger

logger = get_logger(__name__)

from .config import (
    COUNTRY_CURRENCY
)
from .db import random_datetime_between

def generate_orders(conn, generation_date):
    with conn.cursor() as cur:
        cur.execute("SELECT customer_id, country FROM dw.dim_customer WHERE is_current = True")
        res = {row[0]:row[1] for row in cur.fetchall()}

    if not res:
        logger.info("No customers in warehouse yet — skipping order generation")
        return pd.DataFrame()
    
    print("\n[fact_orders] Generating orders...")

    gen_dt      = datetime.combine(generation_date, datetime.min.time()).replace(tzinfo=timezone.utc)
    ingested_at = datetime.now(timezone.utc).isoformat()

    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(DISTINCT customer_id) FROM dw.dim_customer")
        total_customers = cur.fetchone()[0]

    today = generation_date

    # 1. Base daily order rate
    base_rate = random.gauss(0.04, 0.008)   # centered around 4%
    base_rate = max(0.02, min(0.06, base_rate))  # clamp to 2%-6%

    # 2. Day-of-week effect
    weekday_factors = {
        0: 0.95,  # Monday
        1: 1.00,  # Tuesday
        2: 1.02,  # Wednesday
        3: 1.05,  # Thursday
        4: 1.10,  # Friday
        5: 1.18,  # Saturday
        6: 1.12,  # Sunday
    }
    weekday_factor = weekday_factors[today.weekday()]

    # 3. Seasonal/month effect
    if today.month in (10, 12):
        seasonal_factor = 1.20
    elif today.month in (6, 7):
        seasonal_factor = 1.08
    else:
        seasonal_factor = 1.00

    # 4. Small random daily noise
    noise_factor = random.uniform(0.97, 1.03)

    # 5. Final expected orders
    adjusted_rate = base_rate * weekday_factor * seasonal_factor * noise_factor
    estimated_orders = int(total_customers * adjusted_rate)

    # 6. Keep result in a sensible range
    num_orders = min(399, max(179, estimated_orders))
    
    with conn.cursor() as cur:
        cur.execute("SELECT MAX(CAST(SUBSTRING(order_id FROM 5) AS INTEGER)) FROM dw.fact_orders")
        result = cur.fetchone()[0]
    start_index = (result or 0) + 1

    rows = []
    
    for i in range(start_index, start_index + num_orders):
        order_id = f"ORD-{i:05d}"

        order_created_at = random_datetime_between(
            gen_dt,
            gen_dt + timedelta(hours=23)
        )
        order_last_updated_at = order_created_at
        order_status = "created"
        order_channel = random.choice(["web", "mobile", "marketplace"])

        customer_id = random.choice(list(res.keys()))
        country = res[customer_id]
        currency_code = COUNTRY_CURRENCY[country]


        rows.append({
            "order_id":            order_id,
            "customer_id":         customer_id,
            "order_created_at":    order_created_at,
            "order_last_updated_at": order_last_updated_at,  # placeholder
            "order_status":        order_status,
            "order_channel":       order_channel,
            "total_order_amount":  0,           # updated by gen_order_items
            "order_discount_total": 0,          # updated by gen_order_items
            "currency_code":       currency_code,
            "total_order_amount_inr":   None,   # calculated in ETL
            "order_discount_total_inr": None,   # calculated in ETL
            "event_type":          "order_created",
            "ingested_at":         ingested_at,
        })

    df = pd.DataFrame(rows)

    print(f"  Done. {len(df)} orders generated.")
    return df
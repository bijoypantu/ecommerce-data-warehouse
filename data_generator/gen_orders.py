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
        logger.info("  No customers in warehouse yet — skipping order generation")
        return pd.DataFrame()
    
    print("\n[fact_orders] Generating orders...")

    gen_dt      = datetime.combine(generation_date, datetime.min.time()).replace(tzinfo=timezone.utc)
    ingested_at = datetime.now(timezone.utc).isoformat()

    with conn.cursor() as cur:
        cur.execute("""SELECT COUNT(DISTINCT customer_id) FROM dw.dim_customer""")
        total_customers = cur.fetchone()[0]
    
    daily_rate = random.uniform(0.10, 0.20)
    num_orders = max(random.randint(5, 10), int(total_customers * daily_rate))
    
    with conn.cursor() as cur:
        cur.execute("SELECT MAX(CAST(SUBSTRING(order_id FROM 6) AS INTEGER)) FROM dw.fact_orders")
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
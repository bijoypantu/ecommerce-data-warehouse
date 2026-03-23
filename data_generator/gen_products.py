# etl/extract/generator/gen_products.py
# ============================================================
# Generates dim_product DataFrame.
# Pass 1: 1000 active products
# Pass 2: 20% get discontinued via SCD2
# Returns DataFrame with all versions for Bronze layer writing.
# ============================================================

import random
import pandas as pd
from datetime import date, datetime, timezone, timedelta
from faker import Faker
from dateutil.relativedelta import relativedelta

from .config import (
    BRANDS, COLORS, SIZES
)
from .db import random_datetime, random_datetime_between

fake = Faker()



def generate_products(conn, categories_df, generation_date):
    
    print("\n[dim_product] Generating products...")

    gen_dt      = datetime.combine(generation_date, datetime.min.time()).replace(tzinfo=timezone.utc)
    ingested_at = datetime.now(timezone.utc).isoformat()

    sub_categories = list(zip(categories_df["category_id"], categories_df["category_name"]))
    # ----------------------------------------------------------
    # Get current max product number from warehouse
    # so new IDs continue from where we left off
    # ----------------------------------------------------------
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM dw.dim_product")
        count = cur.fetchone()[0]

        if count == 0:
            # First run — seed with 100 products
            num_products = 100
        else:
            # Weekly addition — 3-5 new products
            num_products = random.randint(3, 5)

        cur.execute("SELECT MAX(CAST(SUBSTRING(product_id FROM 6) AS INTEGER)) FROM dw.dim_product")
        result = cur.fetchone()[0]
    start_index = (result or 0) + 1

    rows = []

    # ----------------------------------------------------------
    # PASS 1: Generate 3-5 active products
    # ----------------------------------------------------------
    for i in range(start_index, start_index + num_products):
        product_id   = f"PROD-{i:05d}"
        cat_id, cat_name = random.choice(sub_categories)

        brand        = random.choice(BRANDS.get(cat_name, ["Generic"]))
        color        = random.choice(COLORS)
        size_options = SIZES.get(cat_name)
        size         = random.choice(size_options) if size_options else None
        product_name = f"{brand} {cat_name}"
        model        = fake.bothify(text="Model-##??").upper()
        effective_start = random_datetime_between(
            gen_dt,
            gen_dt + timedelta(hours=23)
        )

        rows.append({
            "product_id":      product_id,
            "product_name":    product_name,
            "brand":           brand,
            "model":           model,
            "color":           color,
            "size":            size,
            "category_id":     cat_id,
            "category_name":   cat_name,
            "product_status":  "active",
            "effective_start": effective_start,
            "event_type":      "created",
            "ingested_at":     datetime.now(timezone.utc).isoformat(),
        })

    print(f"  Pass 1 complete — {num_products} active products generated.")

    # ----------------------------------------------------------
    # PASS 2: SCD2 — discontinue 1-2 products
    # ----------------------------------------------------------
    new_rows = []

    with conn.cursor() as cur:
        cur.execute("""
            SELECT 
                dp.product_id, dp.product_name, dp.brand, dp.model, 
                dp.color, dp.size,
                dc.category_id, dc.category_name
            FROM dw.dim_product dp
            JOIN dw.dim_category dc ON dp.category_sk = dc.category_sk
            WHERE dp.is_current = true
            AND effective_start <= %(gen_date)s::date - INTERVAL '3 months'
            """, {"gen_date": generation_date.isoformat()})
        eligible_products = cur.fetchall()

    if eligible_products:
        num_to_update = random.randint(1, 2)
        products_to_update = random.sample(
            eligible_products,
            min(num_to_update, len(eligible_products))
        )

        change_dt = random_datetime_between(
            gen_dt,
            gen_dt + timedelta(hours=23)
        )

        for row in products_to_update:
            (product_id, product_name, brand, model, color,
             size, category_id, category_name) = row

            new_rows.append({
                "product_id":      product_id,
                "product_name":    product_name,
                "brand":           brand,
                "model":           model,
                "color":           color,
                "size":            size,
                "category_id":     category_id,
                "category_name":   category_name,
                "product_status":  "discontinued",
                "effective_start": change_dt,
                "event_type":      "discontinued",
                "ingested_at":     ingested_at
            })

        print(f"  Pass 2 complete — {num_to_update} products discontinued via SCD2.")
    else:
        print("  Pass 2 — no eligible products for SCD2 yet.")
    
    df = pd.DataFrame(rows)
    df = pd.concat([df, pd.DataFrame(new_rows)], ignore_index=True)
    print(f"  Done. {len(df)} total product rows generated.")
    return df
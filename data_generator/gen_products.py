# etl/extract/generator/gen_products.py
# ============================================================
# Generates dim_product DataFrame.
# Pass 1: 1000 active products
# Pass 2: 20% get discontinued via SCD2
# Returns DataFrame with all versions for Bronze layer writing.
# ============================================================

import random
import pandas as pd
from datetime import date, datetime, timezone
from faker import Faker
from dateutil.relativedelta import relativedelta

from .config import (
    CATEGORIES, BRANDS, COLORS, SIZES,
    NUM_PRODUCTS, SCD2_PROD_RATE
)
from .db import random_datetime, random_datetime_between

fake = Faker()


def get_sub_categories(categories_df):
    """
    Extracts sub-categories from the categories DataFrame.
    Returns list of (category_id, category_name) tuples.
    """
    return [
        (row["category_id"], row["category_name"])
        for _, row in categories_df.iterrows()
        if row["parent_category_id"] is not None
    ]


def generate_products(categories_df):
    """
    Generates dim_product DataFrame in two passes.
    Pass 1 — 1000 active products
    Pass 2 — 20% get discontinued via SCD2
    Returns full DataFrame with all versions.
    """

    print("\n[dim_product] Generating products...")

    sub_categories = get_sub_categories(categories_df)
    rows = []

    # ----------------------------------------------------------
    # PASS 1: Generate 1000 active products
    # ----------------------------------------------------------
    for i in range(1, NUM_PRODUCTS + 1):
        product_id   = f"PROD-{i:05d}"
        cat_id, cat_name = random.choice(sub_categories)

        brand        = random.choice(BRANDS.get(cat_name, ["Generic"]))
        color        = random.choice(COLORS)
        size_options = SIZES.get(cat_name)
        size         = random.choice(size_options) if size_options else None
        product_name = f"{brand} {cat_name}"
        model        = fake.bothify(text="Model-##??").upper()
        effective_start = random_datetime(date(2020, 1, 1), date(2024, 12, 31))

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

    print(f"  Pass 1 complete — {NUM_PRODUCTS} active products generated.")

    # ----------------------------------------------------------
    # PASS 2: SCD2 — discontinue 20% of products
    # ----------------------------------------------------------
    df = pd.DataFrame(rows)

    num_to_discontinue = int(len(df) * SCD2_PROD_RATE)
    products_to_discontinue = df.sample(num_to_discontinue)

    new_rows = []
    for _, old_row in products_to_discontinue.iterrows():
        earliest_change = old_row["effective_start"] + relativedelta(months=6)
        latest_change   = datetime(2025, 12, 31, tzinfo=timezone.utc)

        if earliest_change >= latest_change:
            continue

        change_date = random_datetime_between(earliest_change, latest_change)

        new_rows.append({
            "product_id":      old_row["product_id"],
            "product_name":    old_row["product_name"],
            "brand":           old_row["brand"],
            "model":           old_row["model"],
            "color":           old_row["color"],
            "size":            old_row["size"],
            "category_id":     old_row["category_id"],
            "category_name":   old_row["category_name"],
            "product_status":  "discontinued",
            "effective_start": change_date,
            "event_type":      "discontinued",
            "ingested_at":     datetime.now(timezone.utc).isoformat(),
        })

    df = pd.concat([df, pd.DataFrame(new_rows)], ignore_index=True)

    print(f"  Pass 2 complete — {num_to_discontinue} products discontinued via SCD2.")
    print(f"  Done. {len(df)} total product rows generated.")
    return df
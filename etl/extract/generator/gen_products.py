# etl/extract/generator/gen_products.py
# ============================================================
# Generates and inserts dim_product rows.
# Pass 1: 1000 active products
# Pass 2: 20% get discontinued via SCD2
# Return all product versions for SCD2 lookup in gen_orders.py
# ============================================================

import random
from datetime import date
from faker import Faker

from .config import (
    CATEGORIES, BRANDS, COLORS, SIZES,
    NUM_PRODUCTS, SCD2_PROD_RATE
)
from .db import bulk_insert, fetch_all, execute_many, random_date, random_datetime

fake = Faker()


# ----------------------------------------------------------
# Helper: get sub-categories only from category_map
# Returns list of (category_id, category_name, category_sk)
# ----------------------------------------------------------
def get_sub_categories(category_map):
    sub_cats = [
        (cat_id, cat_name, parent_id)
        for cat_id, cat_name, parent_id in CATEGORIES
        if parent_id is not None
    ]
    return [
        (cat_id, cat_name, category_map[cat_id])
        for cat_id, cat_name, _ in sub_cats
        if cat_id in category_map
    ]


def generate_products(conn, category_map):
    """
    Generates dim_product rows in two passes.
    Pass 1 — 1000 active products
    Pass 2 — 20% get discontinued via SCD2
    Returns all_products for current versions.
    """

    print("\n[dim_product] Generating products...")

    sub_categories = get_sub_categories(category_map)

    # ------------------------------------------------------
    # PASS 1: Generate 1000 active products
    # ------------------------------------------------------
    pass1_rows = []

    for i in range(1, NUM_PRODUCTS + 1):
        product_id = f"PROD-{i:05d}"

        # Pick a random sub-category
        cat_id, cat_name, cat_sk = random.choice(sub_categories)

        # Pick brand, color, size based on sub-category
        brand  = random.choice(BRANDS.get(cat_name, ["Generic"]))
        color  = random.choice(COLORS)
        size_options = SIZES.get(cat_name)
        size   = random.choice(size_options) if size_options else None

        product_name = f"{brand} {cat_name}"
        model        = fake.bothify(text="Model-##??").upper()
        effective_start = random_datetime(date(2020, 1, 1), date(2023, 12, 31))

        pass1_rows.append((
            product_id,
            product_name,
            brand,
            model,
            color,
            size,
            cat_sk,
            "active",
            effective_start,
            None,    # effective_end
            True,    # is_current
        ))

    bulk_insert(
        conn,
        table="dw.dim_product",
        columns=[
            "product_id", "product_name", "brand", "model",
            "color", "size", "category_sk", "product_status",
            "effective_start", "effective_end", "is_current"
        ],
        rows=pass1_rows
    )

    print(f"  Pass 1 complete — {NUM_PRODUCTS} active products inserted.")

    # ------------------------------------------------------
    # PASS 2: SCD2 — discontinue 20% of products
    # ------------------------------------------------------
    all_products = fetch_all(
        conn,
        """
        SELECT product_sk, product_id, product_name, brand, model,
               color, size, category_sk, effective_start
        FROM dw.dim_product
        WHERE is_current = TRUE
        """
    )

    num_to_discontinue = int(len(all_products) * SCD2_PROD_RATE)
    products_to_discontinue = random.sample(all_products, num_to_discontinue)

    update_rows = []
    new_rows    = []

    for row in products_to_discontinue:
        (product_sk, product_id, product_name, brand, model,
         color, size, cat_sk, old_effective_start) = row

        # SCD2 change date — after the product's effective_start
        change_date = random_datetime(date(2024, 1, 1), date(2025, 6, 30))

        # Close the current row
        update_rows.append((change_date, product_sk))

        # Open a new discontinued row
        new_rows.append((
            product_id,
            product_name,
            brand,
            model,
            color,
            size,
            cat_sk,
            "discontinued",
            change_date,   # effective_start = change_date
            None,          # effective_end
            True,          # is_current
        ))

    # Run updates first, then inserts
    execute_many(
        conn,
        """
        UPDATE dw.dim_product
        SET effective_end = %s,
            is_current = FALSE
        WHERE product_sk = %s
        AND is_current = TRUE
        """,
        update_rows
    )

    bulk_insert(
        conn,
        table="dw.dim_product",
        columns=[
            "product_id", "product_name", "brand", "model",
            "color", "size", "category_sk", "product_status",
            "effective_start", "effective_end", "is_current"
        ],
        rows=new_rows
    )

    print(f"  Pass 2 complete — {num_to_discontinue} products discontinued via SCD2.")

    # ------------------------------------------------------
    # Returns all product versions for SCD2 lookup in gen_orders.py
    # ------------------------------------------------------
    all_products = fetch_all(
        conn,
        """
        SELECT dp.product_id, dp.product_sk, dp.effective_start, dp.effective_end,
            dc.category_name
        FROM dw.dim_product dp
        JOIN dw.dim_category dc ON dc.category_sk = dp.category_s
        """
    )

    return all_products
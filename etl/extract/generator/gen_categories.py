# etl/extract/generator/gen_categories.py
# ============================================================
# Generates and inserts dim_category rows.
# No Faker needed — categories are hardcoded in config.py.
# Returns a dict mapping category_id → category_sk
# for use by gen_products.py
# ============================================================

from .config import CATEGORIES
from .db import bulk_insert, fetch_all


def generate_categories(conn):
    """
    Inserts all parent and sub-categories into dw.dim_category.
    Returns category_id_to_sk: dict mapping category_id → category_sk
    """

    print("\n[dim_category] Inserting categories...")

    # ----------------------------------------------------------
    # Step 1: Insert parent categories first (parent_category_sk = NULL)
    # ----------------------------------------------------------
    parent_rows = [
        (cat_id, cat_name)
        for cat_id, cat_name, parent_id in CATEGORIES
        if parent_id is None
    ]

    bulk_insert(
        conn,
        table="dw.dim_category",
        columns=["category_id", "category_name"],
        rows=parent_rows
    )

    # ----------------------------------------------------------
    # Step 2: Fetch the inserted parents to build id → sk map
    # We need their generated category_sk values
    # ----------------------------------------------------------
    inserted = fetch_all(conn, "SELECT category_id, category_sk FROM dw.dim_category")
    category_id_to_sk = {cat_id: cat_sk for cat_id, cat_sk in inserted}

    # ----------------------------------------------------------
    # Step 3: Insert sub-categories, resolving parent_category_sk
    # ----------------------------------------------------------
    sub_rows = [
        (cat_id, cat_name, category_id_to_sk[parent_id])
        for cat_id, cat_name, parent_id in CATEGORIES
        if parent_id is not None
    ]

    bulk_insert(
        conn,
        table="dw.dim_category",
        columns=["category_id", "category_name", "parent_category_sk"],
        rows=sub_rows
    )

    # ----------------------------------------------------------
    # Step 4: Rebuild the full map including sub-categories
    # This gets returned and used by gen_products.py
    # ----------------------------------------------------------
    all_inserted = fetch_all(
        conn,
        "SELECT category_id, category_sk FROM dw.dim_category"
    )
    category_id_to_sk = {cat_id: cat_sk for cat_id, cat_sk in all_inserted}

    print(f"  Done. {len(category_id_to_sk)} categories loaded.")
    return category_id_to_sk
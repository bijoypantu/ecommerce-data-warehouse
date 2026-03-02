# data_generator/run_generator.py
# ============================================================
# Master script — orchestrates all data generation in correct
# dependency order and writes Bronze JSONL files to data_lake/raw/
#
# Run with: python -m data_generator.run_generator
# ============================================================

import pandas as pd
from datetime import datetime

from .db import build_customer_versions, build_product_versions, write_jsonl
from .gen_categories import generate_categories
from .gen_products import generate_products
from .gen_customers import generate_customers
from .gen_orders import generate_orders
from .gen_order_items import generate_order_items
from .gen_payments import generate_payments
from .gen_shipments import generate_shipments
from .gen_refunds import generate_refunds


def main():
    start_time = datetime.now()
    print("=" * 60)
    print("  E-Commerce Data Generator — Bronze Layer")
    print(f"  Started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    # ----------------------------------------------------------
    # DIMENSIONS
    # ----------------------------------------------------------

    # Step 1 — Categories (no dependencies)
    category_df = generate_categories()

    # Step 2 — Products (depends on categories)
    product_df = generate_products(category_df)

    # Step 3 — Customers (no dependencies)
    customer_df = generate_customers()

    # ----------------------------------------------------------
    # BUILD IN-MEMORY VERSION LOOKUPS
    # Used for SCD2 resolution during order generation
    # ----------------------------------------------------------
    customer_versions = build_customer_versions(customer_df)
    product_versions  = build_product_versions(product_df)

    # ----------------------------------------------------------
    # FACT TABLES
    # ----------------------------------------------------------

    # Step 4 — Orders (depends on customer_versions)
    # All orders start as "order_created" events
    orders_df = generate_orders(customer_versions)

    # Step 5 — Build exchange rate lookup from seed CSV
    # Used to convert INR prices to original currency
    rates_df   = pd.read_csv("warehouse/seeds/all_currencies_to_inr.csv")
    rate_lookup = {
        (row["date_sk"], row["currency_code"]): row["rate_to_inr"]
        for _, row in rates_df.iterrows()
    }

    # Step 6 — Order Items (depends on orders, products, exchange rates)
    # Also appends "order_totals_updated" events to orders_df
    items_df, orders_df = generate_order_items(
        orders_df, product_versions, rate_lookup
    )

    # Step 7 — Payments (depends on orders with real totals)
    # Appends "order_cancelled" and "order_processing" events to orders_df
    payments_df, orders_df = generate_payments(orders_df)

    # Step 8 — Shipments (depends on processing orders and items)
    # Appends "order_shipped" and "order_delivered" events to orders_df
    shipments_df, orders_df, delivered_shipments_df = generate_shipments(
        orders_df, items_df
    )

    # Step 9 — Refunds (depends on delivered shipments and items)
    refunds_df = generate_refunds(delivered_shipments_df, items_df)

    # ----------------------------------------------------------
    # WRITE BRONZE JSONL FILES TO DATA LAKE
    # ----------------------------------------------------------
    print("\n" + "=" * 60)
    print("  Writing Bronze JSONL files to data_lake/raw/")
    print("=" * 60)

    write_jsonl(category_df,  "data_lake/raw/dim_category.jsonl")
    write_jsonl(product_df,   "data_lake/raw/dim_product.jsonl")
    write_jsonl(customer_df,  "data_lake/raw/dim_customer.jsonl")
    write_jsonl(orders_df,    "data_lake/raw/fact_orders.jsonl")
    write_jsonl(items_df,     "data_lake/raw/fact_order_items.jsonl")
    write_jsonl(payments_df,  "data_lake/raw/fact_payments.jsonl")
    write_jsonl(shipments_df, "data_lake/raw/fact_shipments.jsonl")
    write_jsonl(refunds_df,   "data_lake/raw/fact_refunds.jsonl")

    # ----------------------------------------------------------
    # SUMMARY
    # ----------------------------------------------------------
    end_time = datetime.now()
    duration = (end_time - start_time).seconds

    print("\n" + "=" * 60)
    print("  Generation Complete")
    print(f"  Categories:   {len(category_df):>8,} rows")
    print(f"  Products:     {len(product_df):>8,} rows")
    print(f"  Customers:    {len(customer_df):>8,} rows")
    print(f"  Orders:       {len(orders_df):>8,} rows (all events)")
    print(f"  Order Items:  {len(items_df):>8,} rows")
    print(f"  Payments:     {len(payments_df):>8,} rows")
    print(f"  Shipments:    {len(shipments_df):>8,} rows")
    print(f"  Refunds:      {len(refunds_df):>8,} rows")
    print(f"\n  Total time:   {duration} seconds")
    print("=" * 60)



if __name__ == "__main__":
    main()
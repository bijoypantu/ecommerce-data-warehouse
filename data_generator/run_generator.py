# data_generator/run_generator.py
# ============================================================
# Master script — orchestrates all data generation in correct
# dependency order and writes Bronze JSONL files to data_lake/raw/
#
# Run with: python -m data_generator.run_generator
#
# Date detection logic:
#   - Queries audit.pipeline_runs for last successful generation date
#   - First run → uses today's date
#   - Subsequent runs → last date + 1 day
#
# Partition logic:
#   - Categories → one-time seed only, skipped if already generated
#   - Products    → weekly (Mondays only)
#   - Customers   → daily (5-20 new customers)
#   - All facts   → daily
# ============================================================

import pandas as pd
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .db import write_jsonl, get_connection
from .gen_categories import generate_categories
from .gen_products import generate_products
from .gen_customers import generate_customers
from .gen_orders import generate_orders
from .gen_order_items import generate_order_items
from .gen_payments import generate_payments
from .gen_shipments import generate_shipments
from .gen_refunds import generate_refunds

from etl.utils.logger import get_logger
from etl.utils.auditor import PipelineAuditor

logger = get_logger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[1]


BRONZE_ROOT = PROJECT_ROOT / "data_lake" / "raw"

def get_generation_date() -> datetime.date:
    partitions = sorted(BRONZE_ROOT.glob("????-??-??"))
    if not partitions:
        next_date = datetime.now(timezone.utc).date()
        logger.info(f"No prior Bronze partition found → using today: {next_date}")
    else:
        last_date = datetime.strptime(partitions[-1].name, "%Y-%m-%d").date()
        next_date = last_date + timedelta(days=1)
        logger.info(f"Last Bronze partition: {last_date} → generating for {next_date}")
    return next_date


def generate():
    start_time = datetime.now()
    logger.info("=" * 60)
    logger.info("  E-Commerce Data Generator — Bronze Layer")
    logger.info(f"  Started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 60)

    # ----------------------------------------------------------
    # Detect generation date from audit table
    # ----------------------------------------------------------
    conn = get_connection()
    generation_date = get_generation_date()

    OUTPUT_DIR = PROJECT_ROOT / "data_lake" / "raw" / generation_date.isoformat()
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    logger.info(f"Output directory: {OUTPUT_DIR}")

    with PipelineAuditor(
        pipeline_name="data_generator",
        table_name="all_tables",
        layer="generation"
    ) as auditor:

        # ----------------------------------------------------------
        # STEP 1 — Categories (one-time seed only)
        # Skip if already exists in any prior partition
        # ----------------------------------------------------------
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM dw.dim_category")
            count = cur.fetchone()[0]

        if count == 0:
            category_df = generate_categories()
            sub_category_df = category_df[category_df["parent_category_id"].notna()][["category_id", "category_name"]]
        else:
            logger.info("[dim_category] Categories already seeded — reading from warehouse...")
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT category_id, category_name
                    FROM dw.dim_category
                    WHERE parent_category_sk IS NOT NULL
                """)
                sub_category_df = pd.DataFrame(
                    cur.fetchall(),
                    columns=["category_id", "category_name"]
                )

        # ----------------------------------------------------------
        # STEP 2 — Products (weekly — Mondays only)
        # ----------------------------------------------------------
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM dw.dim_product")
            product_count = cur.fetchone()[0]

        if generation_date.weekday() == 0 or product_count == 0:
            logger.info("[dim_product] Generating products...")
            product_df = generate_products(conn, sub_category_df, generation_date)
            logger.info(f"[dim_product] Generated {len(product_df)} rows")
        else:
            logger.info("[dim_product] Not Monday — skipping product generation")
            product_df = None

        # ----------------------------------------------------------
        # STEP 3 — Customers (daily — 5-15 new customers)
        # ----------------------------------------------------------
        logger.info("[dim_customer] Generating customers...")
        customer_df = generate_customers(conn, generation_date)
        logger.info(f"[dim_customer] Generated {len(customer_df)} rows")

        # ----------------------------------------------------------
        # STEP 4 — Orders 10-20% of total customers daily
        # ----------------------------------------------------------
        logger.info("[fact_orders] Generating orders...")
        orders_df = generate_orders(conn, generation_date)
        logger.info(f"[fact_orders] Generated {len(orders_df)} order_created events")

        # ----------------------------------------------------------
        # STEP 5 — Exchange rate lookup
        # ----------------------------------------------------------
        rates_df = pd.read_csv(
            PROJECT_ROOT / "warehouse" / "seeds" / "all_currencies_to_inr.csv"
        )
        rate_lookup = {
            (row["date_sk"], row["currency_code"]): row["rate_to_inr"]
            for _, row in rates_df.iterrows()
        }

        if orders_df.empty:
            logger.info("  Skipping order items, payments — no orders generated")
            items_df = pd.DataFrame()
            payments_df = pd.DataFrame()
        else:
            # ----------------------------------------------------------
            # STEP 6 — Order Items
            # Also appends order_totals_updated events to orders_df
            # ----------------------------------------------------------
            logger.info("[fact_order_items] Generating order items...")
            items_df, orders_df = generate_order_items(
                conn, orders_df, rate_lookup
            )
            logger.info(f"[fact_order_items] Generated {len(items_df)} rows")

            # ----------------------------------------------------------
            # STEP 7 — Payments
            # Appends order_cancelled and order_processing events to orders_df
            # ----------------------------------------------------------
            logger.info("[fact_payments] Generating payments...")
            payments_df, orders_df = generate_payments(orders_df)
            logger.info(f"[fact_payments] Generated {len(payments_df)} rows")

        # ----------------------------------------------------------
        # STEP 8 — Shipments
        # Queries warehouse for unshipped orders from previous days
        # Appends order_shipped and order_delivered events to orders_df
        # ----------------------------------------------------------
        logger.info("[fact_shipments] Generating shipments...")
        shipments_df, shipment_order_events_df = generate_shipments(conn, generation_date)
        orders_df = pd.concat([orders_df, shipment_order_events_df], ignore_index=True)
        logger.info(f"[fact_shipments] Generated {len(shipments_df)} rows")

        # ----------------------------------------------------------
        # STEP 9 — Refunds
        # Queries warehouse for delivered orders eligible for refunds
        # ----------------------------------------------------------
        logger.info("[fact_refunds] Generating refunds...")
        refunds_df = generate_refunds(conn, generation_date)
        logger.info(f"[fact_refunds] Generated {len(refunds_df)} rows")

        # ----------------------------------------------------------
        # WRITE BRONZE JSONL FILES TO DATA LAKE
        # ----------------------------------------------------------
        logger.info(f"Writing Bronze JSONL files to {OUTPUT_DIR.relative_to(PROJECT_ROOT)}/")

        if count == 0:
            write_jsonl(category_df, OUTPUT_DIR / "dim_category.jsonl")

        if product_df is not None:
            write_jsonl(product_df, OUTPUT_DIR / "dim_product.jsonl")

        write_jsonl(customer_df,  OUTPUT_DIR / "dim_customer.jsonl")
        write_jsonl(orders_df,    OUTPUT_DIR / "fact_orders.jsonl")
        write_jsonl(items_df,     OUTPUT_DIR / "fact_order_items.jsonl")
        write_jsonl(payments_df,  OUTPUT_DIR / "fact_payments.jsonl")
        write_jsonl(shipments_df, OUTPUT_DIR / "fact_shipments.jsonl")
        write_jsonl(refunds_df,   OUTPUT_DIR / "fact_refunds.jsonl")

        # ----------------------------------------------------------
        # AUDIT — record total rows generated
        # ----------------------------------------------------------
        total_rows = (
            (len(category_df) if count == 0 else 0) +
            (len(product_df) if product_df is not None else 0) +
            len(customer_df) +
            len(orders_df) +
            len(items_df) +
            len(payments_df) +
            len(shipments_df) +
            len(refunds_df)
        )

        auditor.set_row_counts(
            rows_read=0,
            rows_written=total_rows,
            rows_rejected=0
        )

        # ----------------------------------------------------------
        # SUMMARY
        # ----------------------------------------------------------
        end_time = datetime.now()
        duration = (end_time - start_time).seconds

        logger.info("=" * 60)
        logger.info("  Generation Complete")
        logger.info(f"  Generation date: {generation_date}")
        logger.info(f"  Categories:   {(len(category_df) if count == 0 else 0):>8,} rows")
        logger.info(f"  Products:     {len(product_df) if product_df is not None else 0:>8,} rows")
        logger.info(f"  Customers:    {len(customer_df):>8,} rows")
        logger.info(f"  Orders:       {len(orders_df):>8,} rows (all events)")
        logger.info(f"  Order Items:  {len(items_df):>8,} rows")
        logger.info(f"  Payments:     {len(payments_df):>8,} rows")
        logger.info(f"  Shipments:    {len(shipments_df):>8,} rows")
        logger.info(f"  Refunds:      {len(refunds_df):>8,} rows")
        logger.info(f"  Total time:   {duration} seconds")
        logger.info("=" * 60)

    conn.close()

if __name__ == "__main__":
    generate()
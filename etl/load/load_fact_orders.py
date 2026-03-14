import pandas as pd
from pathlib import Path
from psycopg2.extras import execute_values

from etl.extract.read_gold import read_gold
from etl.utils.logger import get_logger
from etl.utils.auditor import PipelineAuditor

logger = get_logger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[2]

def run(conn):
    with PipelineAuditor(
        pipeline_name="load_fact_orders",
        table_name="fact_orders",
        layer="warehouse"
    ) as auditor:
        
        # ------------------------------------------------------
        # STEP 1: Read Gold Parquet
        # ------------------------------------------------------
        try:
            ord_df, _ = read_gold("fact_orders")
        except FileNotFoundError:
            logger.info("fact_orders.parquet not found — skipping")
            return
        rows_read = len(ord_df)
        logger.info(f"Rows read from Silver: {rows_read}")

        # ------------------------------------------------------
        # STEP 2: Insert orders
        # ------------------------------------------------------
        
        # Query all customer versions
        with conn.cursor() as cur:
            cur.execute("""
                SELECT customer_id, customer_sk, effective_start, effective_end
                FROM dw.dim_customer
            """)
            cust_versions = pd.DataFrame(cur.fetchall(), 
                columns=["customer_id", "customer_sk", "effective_start", "effective_end"])

        order_customer_versions = ord_df.merge(
            cust_versions,
            on="customer_id",
            how="left"
        )
        valid_customer_version_mask = (
            (order_customer_versions["order_created_at"] >= order_customer_versions["effective_start"]) &
            (
                order_customer_versions["effective_end"].isna() |
                (order_customer_versions["order_created_at"] < order_customer_versions["effective_end"])
            )
        )
        customer_sk_lookup = order_customer_versions.loc[
            valid_customer_version_mask,
            ["order_id", "customer_sk"]
        ]

        ord_df = ord_df.merge(customer_sk_lookup, on="order_id", how="left")

        ord_rows = list(
            ord_df[[
                "order_id", "customer_sk", "date_sk",
                "order_created_at", "order_last_updated_at",
                "order_status", "order_channel",
                "total_order_amount", "order_discount_total",
                "currency_code", "total_order_amount_inr",
                "order_discount_total_inr"
            ]].itertuples(index=False, name=None)
        )

        insert_sql = """
            INSERT INTO dw.fact_orders(
                order_id, customer_sk, date_sk,
                order_created_at, order_last_updated_at,
                order_status, order_channel,
                total_order_amount, order_discount_total,
                currency_code, total_order_amount_inr,
                order_discount_total_inr
            )
            VALUES %s
            ON CONFLICT (order_id) DO NOTHING
        """

        try:
            with conn.cursor() as cur:
                execute_values(cur, insert_sql, ord_rows)
            conn.commit()
            logger.info(f"Insert complete — orders inserted")
        except Exception:
            conn.rollback()
            logger.exception("Failed to insert orders")
            raise

        # ------------------------------------------------------
        # STEP 3: Final row count
        # ------------------------------------------------------
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM dw.fact_orders")
            rows_written = cur.fetchone()[0]
        
        logger.info(f"fact_orders load complete | rows_in_warehouse={rows_written}")

        auditor.set_row_counts(
            rows_read=rows_read,
            rows_written=rows_written,
            rows_rejected=0
        )

if __name__ == "__main__":
    from etl.utils.auditor import _get_connection
    conn = _get_connection()
    try:
        run(conn)
    finally:
        conn.close()
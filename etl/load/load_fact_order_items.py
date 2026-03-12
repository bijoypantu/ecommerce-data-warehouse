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
        pipeline_name="load_fact_order_items",
        table_name="fact_order_items",
        layer="warehouse"
    ) as auditor:
        
        # ------------------------------------------------------
        # STEP 1: Read Gold Parquet
        # ------------------------------------------------------
        item_df, execution_date = read_gold("fact_order_items")
        rows_read = len(item_df)
        logger.info(f"Rows read from Gold: {rows_read}")

        # ------------------------------------------------------
        # STEP 2: Insert order items
        # ------------------------------------------------------
        
        # Query all product versions and orders sk's
        with conn.cursor() as cur:
            # Fetch the products SCD2 data
            cur.execute("""
                SELECT product_id, product_sk, effective_start, effective_end
                FROM dw.dim_product
            """)
            prod_versions = pd.DataFrame(cur.fetchall(), 
                columns=["product_id", "product_sk", "effective_start", "effective_end"])
            
            # Fetch the order_sk and customer_sk
            cur.execute("""
                SELECT order_id, order_sk, customer_sk, order_created_at
                FROM dw.fact_orders
            """)
            ord_lookup = {row[0]: (row[1], row[2], row[3]) for row in cur.fetchall()}

        item_df["order_sk"] = item_df["order_id"].map(lambda x: ord_lookup[x][0])
        item_df["customer_sk"] = item_df["order_id"].map(lambda x: ord_lookup[x][1])
        item_df["order_created_at"] = item_df["order_id"].map(lambda x: ord_lookup[x][2])


        item_prod_versions = item_df.merge(
            prod_versions,
            on="product_id",
            how="left"
        )


        valid_prod_version_mask = (
            (item_prod_versions["order_created_at"] >= item_prod_versions["effective_start"]) &
            (
                item_prod_versions["effective_end"].isna() |
                (item_prod_versions["order_created_at"] < item_prod_versions["effective_end"])
            )
        )
        prod_sk_lookup = item_prod_versions.loc[
            valid_prod_version_mask,
            ["order_item_id", "product_sk"]
        ]

        item_df = item_df.merge(prod_sk_lookup, on="order_item_id", how="left")

        item_rows = list(
            item_df[[
                "order_item_id", "order_sk", "product_sk",
                "customer_sk", "date_sk", "quantity",
                "unit_price_at_order", "discount_amount",
                "line_total_amount", "line_total_amount_inr"
            ]].itertuples(index=False, name=None)
        )

        insert_sql = """
            INSERT INTO dw.fact_order_items(
                order_item_id, order_sk, product_sk,
                customer_sk, date_sk, quantity,
                unit_price_at_order, discount_amount,
                line_total_amount, line_total_amount_inr
            )
            VALUES %s
            ON CONFLICT (order_sk, order_item_id) DO NOTHING
        """

        try:
            with conn.cursor() as cur:
                execute_values(cur, insert_sql, item_rows)
            conn.commit()
            logger.info(f"Insert complete — order items inserted")
        except Exception:
            conn.rollback()
            logger.exception("Failed to insert order items")
            raise

        # ------------------------------------------------------
        # STEP 3: Final row count
        # ------------------------------------------------------
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM dw.fact_order_items")
            rows_written = cur.fetchone()[0]
        
        logger.info(f"fact_order_items load complete | rows_in_warehouse={rows_written}")

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
from pathlib import Path
from psycopg2.extras import execute_values
import pandas as pd

from etl.utils.logger import get_logger
from etl.utils.auditor import PipelineAuditor

DATA_PATH = Path("data_lake/curated/fact_refunds.parquet")

logger = get_logger(__name__)

def run(conn):
    with PipelineAuditor(
        pipeline_name="load_fact_refunds",
        table_name="fact_refunds",
        layer="warehouse"
    ) as auditor:
        
        # ------------------------------------------------------
        # STEP 1: Read Gold Parquet
        # ------------------------------------------------------
        ref_df = pd.read_parquet(DATA_PATH)
        rows_read = len(ref_df)
        logger.info(f"Rows read from Gold: {rows_read}")

        # ------------------------------------------------------
        # STEP 2: Insert refund records
        # ------------------------------------------------------

        # Query order_item_sk, order_sk and customer_sk for each order_item_id
        with conn.cursor() as cur:
            cur.execute("""SELECT order_item_id, order_item_sk, order_sk, customer_sk FROM dw.fact_order_items""")
            ord_lookup = {row[0]: (row[1], row[2], row[3]) for row in cur.fetchall()}

        ref_df["order_item_sk"] = ref_df["order_item_id"].map(lambda x: ord_lookup[x][0])
        ref_df["order_sk"] = ref_df["order_item_id"].map(lambda x: ord_lookup[x][1])
        ref_df["customer_sk"] = ref_df["order_item_id"].map(lambda x: ord_lookup[x][2])

        # Got error for NaT. Convert every NaT to None
        ref_df["processed_at"] = ref_df["processed_at"].astype(object).where(ref_df["processed_at"].notna(), None)

        ref_rows = list(
            ref_df[[
                "refund_id", "order_sk", "order_item_sk",
                "customer_sk", "refund_date_sk", "initiated_at",
                "processed_at", "refund_quantity", "refund_amount",
                "refund_reason", "refund_status", "currency_code",
                "refund_amount_inr"
            ]].itertuples(index=False, name=None)
        )

        insert_sql = """
            INSERT INTO dw.fact_refunds (
                refund_id, order_sk, order_item_sk,
                customer_sk, refund_date_sk, initiated_at,
                processed_at, refund_quantity, refund_amount,
                refund_reason, refund_status, currency_code,
                refund_amount_inr
            )
            VALUES %s
            ON CONFLICT (order_item_sk, refund_id) DO NOTHING
        """

        try:
            with conn.cursor() as cur:
                execute_values(cur, insert_sql, ref_rows)
            conn.commit()
            logger.info(f"Insert complete — refund records inserted")
        except Exception:
            conn.rollback()
            logger.exception("Failed to insert refund records")
            raise

        # ------------------------------------------------------
        # STEP 3: Final row count
        # ------------------------------------------------------
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM dw.fact_refunds")
            rows_written = cur.fetchone()[0]
        
        logger.info(f"fact_refunds load complete | rows_in_warehouse={rows_written}")

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
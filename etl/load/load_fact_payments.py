from pathlib import Path
from psycopg2.extras import execute_values
import pandas as pd

from etl.utils.logger import get_logger
from etl.utils.auditor import PipelineAuditor

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_PATH = PROJECT_ROOT / "data_lake" / "curated" / "fact_payments.parquet"

logger = get_logger(__name__)

def run(conn):
    with PipelineAuditor(
        pipeline_name="load_fact_payments",
        table_name="fact_payments",
        layer="warehouse"
    ) as auditor:
        
        # ------------------------------------------------------
        # STEP 1: Read Gold Parquet
        # ------------------------------------------------------
        pay_df = pd.read_parquet(DATA_PATH)
        rows_read = len(pay_df)
        logger.info(f"Rows read from Gold: {rows_read}")

        # ------------------------------------------------------
        # STEP 2: Insert payment records
        # ------------------------------------------------------

        # Query order_sk and customer_sk for each order_id
        with conn.cursor() as cur:
            cur.execute("""SELECT order_id, order_sk, customer_sk FROM dw.fact_orders""")
            ord_lookup = {row[0]: (row[1], row[2]) for row in cur.fetchall()}

        pay_df["order_sk"] = pay_df["order_id"].map(lambda x: ord_lookup[x][0])
        pay_df["customer_sk"] = pay_df["order_id"].map(lambda x: ord_lookup[x][1])

        pay_rows = list(
            pay_df[[
                "payment_attempt_id", "order_sk", "customer_sk",
                "payment_date_sk", "payment_timestamp",
                "payment_method", "payment_provider", "payment_status",
                "gateway_response_code", "amount", "currency_code",
                "amount_inr"
            ]].itertuples(index=False, name=None)
        )

        insert_sql = """
            INSERT INTO dw.fact_payments (
                payment_attempt_id, order_sk, customer_sk,
                payment_date_sk, payment_timestamp,
                payment_method, payment_provider, payment_status,
                gateway_response_code, amount, currency_code,
                amount_inr
            )
            VALUES %s
            ON CONFLICT (order_sk, payment_attempt_id) DO NOTHING
        """

        try:
            with conn.cursor() as cur:
                execute_values(cur, insert_sql, pay_rows)
            conn.commit()
            logger.info(f"Insert complete — payment records inserted")
        except Exception:
            conn.rollback()
            logger.exception("Failed to insert payment records")
            raise

        # ------------------------------------------------------
        # STEP 3: Final row count
        # ------------------------------------------------------
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM dw.fact_payments")
            rows_written = cur.fetchone()[0]
        
        logger.info(f"fact_payments load complete | rows_in_warehouse={rows_written}")

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
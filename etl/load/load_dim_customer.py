# etl/load/load_dim_customer.py
# ============================================================
# Warehouse loader for dim_customer.
#
# Source  →  data_lake/processed/dim_customer.parquet
# Target  →  dw.dim_customer (PostgreSQL)
#
# Responsibilities:
#   1. Read Silver dim_customer Parquet
#   2. insert all customers with SCD logic
#   3. ON CONFLICT (customer_id, effective_start) DO NOTHING — idempotent
#   4. Track via PipelineAuditor
# ============================================================
import pandas as pd
from pathlib import Path

from etl.utils.logger import get_logger
from etl.utils.auditor import PipelineAuditor

logger = get_logger(__name__)

DATA_PATH = Path("data_lake/processed/dim_customer.parquet")

def run(conn):
    with PipelineAuditor(
        pipeline_name="load_dim_customer",
        table_name="dim_customer",
        layer="warehouse"
    ) as auditor:
        
        # ------------------------------------------------------
        # STEP 1: Read Silver Parquet
        # ------------------------------------------------------
        cust_df = pd.read_parquet(DATA_PATH)
        rows_read = len(cust_df)
        logger.info(f"Rows read from Silver: {rows_read}")

        cust_df = cust_df.sort_values(
            ["customer_id", "effective_start"]
        ).reset_index(drop=True)

        # ------------------------------------------------------
        # STEP 2: Insert customers
        # ------------------------------------------------------
        cust_df["effective_end"] = None
        cust_df["is_current"] = True

        insert_sql = """
            INSERT INTO dw.dim_customer (
                customer_id, first_name, last_name,
                date_of_birth, email, mobile_no,
                city, state, country, signup_timestamp,
                effective_start, effective_end,
                is_current, gender
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (customer_id, effective_start) DO NOTHING
        """
        fetch_sql = """
            SELECT customer_id, customer_sk
            FROM dw.dim_customer
            WHERE is_current = TRUE
        """
        update_sql = """
            UPDATE dw.dim_customer
            SET effective_end = %s,
                is_current = FALSE
            WHERE customer_id = %s
            AND is_current = TRUE
            AND effective_start < %s
        """
        try:
            with conn.cursor() as cur:
                cur.execute(fetch_sql)
                cust_lookup = {row[0]: row[1] for row in cur.fetchall()}
                for _, row in cust_df.iterrows():
                    if row["customer_id"] in cust_lookup:
                        cur.execute(update_sql, (
                            row["effective_start"],
                            row["customer_id"],
                            row["effective_start"]
                        ))
                
                    cur.execute(insert_sql, (
                        row["customer_id"], row["first_name"],
                        row["last_name"], row["date_of_birth"],
                        row["email"], row["mobile_no"], row["city"],
                        row["state"], row["country"],
                        row["signup_timestamp"], row["effective_start"],
                        row["effective_end"], row["is_current"],
                        row["gender"]
                    ))

                    cur.execute(
                        "SELECT customer_sk FROM dw.dim_customer WHERE customer_id=%s AND is_current=TRUE",
                        (row["customer_id"],)
                    )
                    result = cur.fetchone()
                    if result:
                        cust_lookup[row["customer_id"]] = result[0]
            conn.commit()
            logger.info(f"Insert complete — customers inserted")
        except Exception:
            conn.rollback()
            logger.exception("Failed to insert customers")
            raise

        # ------------------------------------------------------
        # STEP 3: Final row count
        # ------------------------------------------------------
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM dw.dim_customer")
            rows_written = cur.fetchone()[0]
        
        logger.info(f"dim_customer load complete | rows_in_warehouse={rows_written}")

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
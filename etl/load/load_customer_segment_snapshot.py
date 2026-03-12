from pathlib import Path
from psycopg2.extras import execute_values
import pandas as pd

from etl.extract.read_gold import read_gold
from etl.utils.logger import get_logger
from etl.utils.auditor import PipelineAuditor

PROJECT_ROOT = Path(__file__).resolve().parents[2]

logger = get_logger(__name__)

def run(conn):
    with PipelineAuditor(
        pipeline_name="load_fact_customer_segment_snapshot",
        table_name="fact_customer_segment_snapshot",
        layer="warehouse"
    ) as auditor:
        
        # ------------------------------------------------------
        # STEP 1: Read Gold Parquet
        # ------------------------------------------------------
        segment_df, execution_date = read_gold("fact_customer_segment_snapshot")
        rows_read = len(segment_df)
        logger.info(f"Rows read from Gold: {rows_read}")
        segment_df["snapshot_month"] = pd.to_datetime(segment_df["snapshot_month"])
        segment_df["created_at"] = pd.Timestamp.now()
        # ------------------------------------------------------
        # STEP 2: Insert refund records
        # ------------------------------------------------------

        # Query all customer versions
        with conn.cursor() as cur:
            cur.execute("""
                SELECT customer_id, customer_sk, effective_start, effective_end
                FROM dw.dim_customer
            """)
            cust_versions = pd.DataFrame(cur.fetchall(), 
                columns=["customer_id", "customer_sk", "effective_start", "effective_end"])

        cust_versions["effective_start"] = cust_versions["effective_start"].dt.tz_localize(None)
        cust_versions["effective_end"] = cust_versions["effective_end"].dt.tz_localize(None)

        order_customer_versions = segment_df.merge(
            cust_versions,
            on="customer_id",
            how="left"
        )
        valid_customer_version_mask = (
            (order_customer_versions["snapshot_month"] >= order_customer_versions["effective_start"]) &
            (
                order_customer_versions["effective_end"].isna() |
                (order_customer_versions["snapshot_month"] < order_customer_versions["effective_end"])
            )
        )
        customer_sk_lookup = order_customer_versions.loc[
            valid_customer_version_mask,
            ["customer_id", "snapshot_month", "customer_sk"]
        ]
        segment_df = segment_df.merge(customer_sk_lookup, on=["customer_id", "snapshot_month"], how="left")

        segment_rows = list(
            segment_df[[
                "snapshot_month", "customer_sk", "revenue_ltm_inr",
                "order_count_ltm", "revenue_percentile", 
                "frequency_percentile", "composite_score",
                "segment_label", "created_at"
            ]].itertuples(index=False, name=None)
        )

        insert_sql = """
            INSERT INTO dw.fact_customer_segment_snapshot (
                snapshot_month, customer_sk, revenue_ltm_inr,
                order_count_ltm, revenue_percentile, 
                frequency_percentile, composite_score,
                segment_label, created_at
            )
            VALUES %s
            ON CONFLICT (snapshot_month, customer_sk) DO NOTHING
        """

        try:
            with conn.cursor() as cur:
                execute_values(cur, insert_sql, segment_rows)
            conn.commit()
            logger.info(f"Insert complete — customer segment records inserted")
        except Exception:
            conn.rollback()
            logger.exception("Failed to insert customer segment records")
            raise

        # ------------------------------------------------------
        # STEP 3: Final row count
        # ------------------------------------------------------
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM dw.fact_customer_segment_snapshot")
            rows_written = cur.fetchone()[0]
        
        logger.info(f"fact_customer_segment_snapshot load complete | rows_in_warehouse={rows_written}")

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
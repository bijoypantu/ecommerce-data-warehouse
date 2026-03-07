from pathlib import Path
from psycopg2.extras import execute_values
import pandas as pd

from etl.utils.logger import get_logger
from etl.utils.auditor import PipelineAuditor

DATA_PATH = Path("data_lake/processed/fact_shipments.parquet")

logger = get_logger(__name__)

def run(conn):
    with PipelineAuditor(
        pipeline_name="load_fact_shipments",
        table_name="fact_shipments",
        layer="warehouse"
    ) as auditor:
        
        # ------------------------------------------------------
        # STEP 1: Read Silver Parquet
        # ------------------------------------------------------
        ship_df = pd.read_parquet(DATA_PATH)
        rows_read = len(ship_df)
        logger.info(f"Rows read from Silver: {rows_read}")

        # ------------------------------------------------------
        # STEP 2: Insert shipment records
        # ------------------------------------------------------

        # Query order_item_sk, order_sk and customer_sk for each order_item_id
        with conn.cursor() as cur:
            cur.execute("""SELECT order_item_id, order_item_sk, order_sk, customer_sk FROM dw.fact_order_items""")
            ord_lookup = {row[0]: (row[1], row[2], row[3]) for row in cur.fetchall()}

        ship_df["order_item_sk"] = ship_df["order_item_id"].map(lambda x: ord_lookup[x][0])
        ship_df["order_sk"] = ship_df["order_item_id"].map(lambda x: ord_lookup[x][1])
        ship_df["customer_sk"] = ship_df["order_item_id"].map(lambda x: ord_lookup[x][2])

        # Got error for NaT. Convert every NaT to None
        ship_df["delivered_at"] = ship_df["delivered_at"].astype(object).where(ship_df["delivered_at"].notna(), None)
        ship_df["delivery_date_sk"] = ship_df["delivery_date_sk"].astype(object).where(ship_df["delivery_date_sk"].notna(), None)

        ship_rows = list(
            ship_df[[
                "shipment_id", "order_sk", "order_item_sk", "customer_sk",
                "shipment_date_sk", "delivery_date_sk", "shipped_at",
                "delivered_at", "shipment_status", "carrier",
                "tracking_id", "shipped_quantity"
            ]].itertuples(index=False, name=None)
        )

        insert_sql = """
            INSERT INTO dw.fact_shipments (
                shipment_id, order_sk, order_item_sk, customer_sk,
                shipment_date_sk, delivery_date_sk, shipped_at,
                delivered_at, shipment_status, carrier,
                tracking_id, shipped_quantity
            )
            VALUES %s
            ON CONFLICT (order_item_sk, shipment_id) DO NOTHING
        """

        try:
            with conn.cursor() as cur:
                execute_values(cur, insert_sql, ship_rows)
            conn.commit()
            logger.info(f"Insert complete — shipment records inserted")
        except Exception:
            conn.rollback()
            logger.exception("Failed to insert shipment records")
            raise

        # ------------------------------------------------------
        # STEP 3: Final row count
        # ------------------------------------------------------
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM dw.fact_shipments")
            rows_written = cur.fetchone()[0]
        
        logger.info(f"fact_shipments load complete | rows_in_warehouse={rows_written}")

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
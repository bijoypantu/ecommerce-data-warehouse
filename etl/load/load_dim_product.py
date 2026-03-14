from pathlib import Path
import pandas as pd

from etl.extract.read_silver import read_silver
from etl.utils.logger import get_logger
from etl.utils.auditor import PipelineAuditor

logger = get_logger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[2]

def run(conn):
    with PipelineAuditor(
        pipeline_name="load_dim_product",
        table_name="dim_product",
        layer="warehouse"
    ) as auditor:
        
        # ------------------------------------------------------
        # STEP 1: Read Silver Parquet
        # ------------------------------------------------------
        try:
            prod_df, _ = read_silver("dim_product")
        except FileNotFoundError:
            logger.info("dim_product.parquet not found — skipping")
            return
        rows_read = len(prod_df)
        logger.info(f"Rows read from Silver: {rows_read}")

        prod_df = prod_df.sort_values(
            ["product_id", "effective_start"]
        ).reset_index(drop=True)

        # ------------------------------------------------------
        # STEP 2: Insert products
        # ------------------------------------------------------
        with conn.cursor() as cur:
            cur.execute("""SELECT category_id, category_sk FROM dw.dim_category""")
            cat_lookup = {row[0]: row[1] for row in cur.fetchall()}
        
        prod_df["effective_end"] = None
        prod_df["is_current"] = True
        prod_df["category_sk"] = prod_df["category_id"].map(cat_lookup)

        insert_sql = """
            INSERT INTO dw.dim_product(
                product_id, product_name,
                brand, model, color, size,
                category_sk, product_status,
                effective_start, effective_end,
                is_current
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (product_id, effective_start) DO NOTHING
        """
        fetch_sql = """
            SELECT product_id, product_sk
            FROM dw.dim_product
            WHERE is_current = TRUE
        """
        update_sql = """
            UPDATE dw.dim_product
            SET effective_end = %s,
                is_current = FALSE
            WHERE product_id = %s
                AND is_current = TRUE
                AND effective_start < %s
        """

        try:
            with conn.cursor() as cur:
                cur.execute(fetch_sql)
                prod_lookup = {row[0]: row[1] for row in cur.fetchall()}
                for _, row in prod_df.iterrows():
                    if row["product_id"] in prod_lookup:
                        cur.execute(update_sql, (
                            row["effective_start"],
                            row["product_id"],
                            row["effective_start"]
                        ))
                    
                    cur.execute(insert_sql, (
                        row["product_id"], row["product_name"],
                        row["brand"], row["model"], row["color"],
                        row["size"], row["category_sk"],
                        row["product_status"], row["effective_start"],
                        row["effective_end"], row["is_current"]
                    ))

                    cur.execute(
                        "SELECT product_sk FROM dw.dim_product WHERE product_id=%s AND is_current=TRUE",
                        (row["product_id"],)
                    )
                    result = cur.fetchone()
                    if result:
                        prod_lookup[row["product_id"]] = result[0]
            conn.commit()
            logger.info(f"Insert complete — products inserted")
        except Exception:
            conn.rollback()
            logger.exception("Failed to insert products")
            raise

        # ------------------------------------------------------
        # STEP 3: Final row count
        # ------------------------------------------------------
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM dw.dim_product")
            rows_written = cur.fetchone()[0]
        
        logger.info(f"dim_product load complete | rows_in_warehouse={rows_written}")

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
# etl/load/load_dim_category.py
# ============================================================
# Warehouse loader for dim_category.
#
# Source  →  data_lake/processed/dim_category.parquet
# Target  →  dw.dim_category (PostgreSQL)
#
# Responsibilities:
#   1. Read Silver dim_category Parquet
#   2. Pass 1 — insert parent categories (parent_category_id IS NULL)
#   3. Query back all SKs → build category_lookup
#   4. Pass 2 — resolve parent_category_sk, insert child categories
#   5. ON CONFLICT (category_id) DO NOTHING — idempotent
#   6. Track via PipelineAuditor
# ============================================================

from pathlib import Path
import pandas as pd
from psycopg2.extras import execute_values

from etl.extract.read_silver import read_silver
from etl.utils.logger import get_logger
from etl.utils.auditor import PipelineAuditor

logger = get_logger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[2]

def run(conn):
    with PipelineAuditor(
        pipeline_name="load_dim_category",
        table_name="dim_category",
        layer="warehouse"
    ) as auditor:

        # ------------------------------------------------------
        # STEP 1: Read Silver Parquet
        # ------------------------------------------------------
        try:
            df, _ = read_silver("dim_category")
        except FileNotFoundError:
            logger.info("dim_category.parquet not found — skipping")
            return
        rows_read = len(df)
        logger.info(f"Rows read from Silver: {rows_read}")

        parent_df = df[df["parent_category_id"].isna()].copy()
        child_df  = df[df["parent_category_id"].notna()].copy()
        logger.info(f"Parents: {len(parent_df)} | Children: {len(child_df)}")

        # ------------------------------------------------------
        # STEP 2: Pass 1 — Insert parent categories
        # parent_category_sk is NULL for top-level categories
        # ------------------------------------------------------
        parent_rows = list(
            parent_df[["category_id", "category_name"]]
            .itertuples(index=False, name=None)
        )

        sql_parents = """
            INSERT INTO dw.dim_category (category_id, category_name)
            VALUES %s
            ON CONFLICT (category_id) DO NOTHING
        """

        try:
            with conn.cursor() as cur:
                execute_values(cur, sql_parents, parent_rows)
            conn.commit()
            logger.info(f"Pass 1 complete — parent categories inserted")
        except Exception:
            conn.rollback()
            logger.exception("Failed to insert parent categories")
            raise

        # ------------------------------------------------------
        # STEP 3: Query back all SKs — build category_lookup
        #
        # ON CONFLICT DO NOTHING returns nothing for existing rows.
        # Querying after insert guarantees a complete lookup whether
        # rows were freshly inserted or already existed.
        # ------------------------------------------------------
        with conn.cursor() as cur:
            cur.execute("SELECT category_id, category_sk FROM dw.dim_category")
            category_lookup = {row[0]: row[1] for row in cur.fetchall()}

        logger.info(f"Category lookup built — {len(category_lookup)} entries")

        # ------------------------------------------------------
        # STEP 4: Pass 2 — Resolve parent SKs, insert children
        # ------------------------------------------------------
        child_df["parent_category_sk"] = child_df["parent_category_id"].map(category_lookup)

        # Guard — every child must have a resolved parent SK
        missing = child_df[child_df["parent_category_sk"].isna()]
        if not missing.empty:
            raise ValueError(
                f"Unresolved parent_category_sk for: {missing['parent_category_id'].unique()}"
            )

        # Cast to int — .map() returns float when NaN was possible
        child_df["parent_category_sk"] = child_df["parent_category_sk"].astype(int)

        child_rows = list(
            child_df[["category_id", "category_name", "parent_category_sk"]]
            .itertuples(index=False, name=None)
        )

        sql_children = """
            INSERT INTO dw.dim_category (category_id, category_name, parent_category_sk)
            VALUES %s
            ON CONFLICT (category_id) DO NOTHING
        """

        try:
            with conn.cursor() as cur:
                execute_values(cur, sql_children, child_rows)
            conn.commit()
            logger.info(f"Pass 2 complete — child categories inserted")
        except Exception:
            conn.rollback()
            logger.exception("Failed to insert child categories")
            raise

        # ------------------------------------------------------
        # STEP 5: Final row count
        # ------------------------------------------------------
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM dw.dim_category")
            rows_written = cur.fetchone()[0]

        logger.info(f"dim_category load complete | rows_in_warehouse={rows_written}")

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
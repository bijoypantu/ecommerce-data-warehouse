# etl/transform/silver/silver_dim_product.py
# ============================================================
# Silver transform for dim_product.
#
# Bronze  →  data_lake/raw/dim_product.jsonl
# Silver  →  data_lake/processed/dim_product.parquet
#
# Responsibilities:
#   1. Read Bronze JSONL via read_bronze()
#   2. Deduplicate on product_id, effective_start
#   3. Validate — nulls and constraints
#   4. Select only Silver-relevant columns
#   5. Write clean Parquet to Silver layer
#   6. Track everything via PipelineAuditor
# ============================================================

from pathlib import Path
import pandas as pd

from etl.extract.read_bronze import read_bronze
from etl.utils.logger import get_logger
from etl.utils.auditor import PipelineAuditor

logger = get_logger(__name__)


PROJECT_ROOT = Path(__file__).resolve().parents[3]
OUTPUT_PATH = PROJECT_ROOT / "data_lake" / "processed" / "dim_product.parquet"

def run():
    with PipelineAuditor (
        pipeline_name="silver_dim_product",
        table_name="dim_product",
        layer="silver"
    ) as auditor:
        
        # ------------------------------------------------------
        # STEP 1: Read Bronze
        # ------------------------------------------------------
        df = read_bronze("dim_product")
        rows_read = len(df)
        logger.info(f"Rows read from Bronze: {rows_read}")
        
        # ------------------------------------------------------
        # STEP 2: Deduplicate on product_id, effective_start
        # ------------------------------------------------------
        before_dedup = rows_read
        df = df.drop_duplicates(subset=["product_id", "effective_start"], keep="first")
        after_dedup = len(df)

        dupes_dropped = before_dedup - after_dedup
        if dupes_dropped > 0:
            logger.warning(f"Duplicates dropped: {dupes_dropped}")

        # ------------------------------------------------------
        # STEP 3: Validate — NOT NULL columns 
        #                   and check constrains
        # ------------------------------------------------------
        null_mask = df["product_id"].isna() | df["product_name"].isna() | df["brand"].isna() | df["model"].isna() | df["category_id"].isna() \
                    | df["product_status"].isna() | df["effective_start"].isna()
        status_mask = ~df["product_status"].str.strip().str.lower().isin(["active", "discontinued"])
        reject_mask = null_mask | status_mask
        rejected_df = df[reject_mask]
        df = df[~reject_mask].reset_index(drop=True)

        rows_rejected = len(rejected_df)

        for _, row in rejected_df.iterrows():
            auditor.log_rejected_record(
                record_id=str(row.get("product_id", "UNKNOWN")),
                rejection_reason= f"null values or wrong status",
                raw_data=row.to_dict()
            )
        
        auditor.log_quality_check(
            check_name="no_null_values_or_wrong_status",
            rows_checked=before_dedup,
            rows_failed=rows_rejected
        )

        logger.info(
            f"Validation complete | "
            f"passed={len(df)} rejected={rows_rejected}"
        )

        # ------------------------------------------------------
        # STEP 4: Select Silver columns
        # ------------------------------------------------------
        df = df[["product_id", "product_name", "brand", "model",
                 "color", "size", "category_id", "product_status",
                 "effective_start"
        ]]

        # ------------------------------------------------------
        # STEP 5: Write to Silver as Parquet
        # ------------------------------------------------------
        OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(OUTPUT_PATH, index=False)

        rows_written = len(df)
        logger.info(f"Silver Parquet written: {OUTPUT_PATH} | rows={rows_written}")

        # ------------------------------------------------------
        # STEP 6: Tell the auditor the final row counts.
        # ------------------------------------------------------
        auditor.set_row_counts(
            rows_read=rows_read,
            rows_written=rows_written,
            rows_rejected=rows_rejected
        )

        logger.info(
            f"silver_dim_product complete |"
            f"read={rows_read} written={rows_written} rejected={rows_rejected}"
        )

# ----------------------------------------------------------------------------
# Allows running directly: python -m etl.transform.silver.silver_dim_product
# ----------------------------------------------------------------------------
if __name__ == "__main__":
    run()
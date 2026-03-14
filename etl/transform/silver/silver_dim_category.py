# etl/transform/silver/silver_dim_category.py
# ============================================================
# Silver transform for dim_category.
#
# Bronze  →  data_lake/raw/dim_category.jsonl
# Silver  →  data_lake/processed/dim_category.parquet
#
# Responsibilities:
#   1. Read Bronze JSONL via read_bronze()
#   2. Deduplicate on category_id
#   3. Validate — category_id and category_name must not be null
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

# ------------------------------------------------------------
# Silver output path — single constant, easy to change
# ------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[3]

def run():

    # ----------------------------------------------------------
    # Start audit run — every pipeline execution is tracked.
    # The 'with' block guarantees end() is always called,
    # even if an exception is raised halfway through.
    # ----------------------------------------------------------
    with PipelineAuditor(
        pipeline_name="silver_dim_category",
        table_name="dim_category",
        layer="silver"
    ) as auditor:

        # ------------------------------------------------------
        # STEP 1: Read Bronze
        # dim_category only has one event type: "created"
        # No filter needed — all rows are valid for Silver
        # ------------------------------------------------------
        try:
            df, execution_date = read_bronze("dim_category")
        except FileNotFoundError:
            logger.info("dim_category.jsonl not found for this date — skipping")
            return
        rows_read = len(df)
        logger.info(f"Rows read from Bronze: {rows_read}")

        OUTPUT_PATH = PROJECT_ROOT / "data_lake" / "processed" / execution_date / "dim_category.parquet"

        # ------------------------------------------------------
        # STEP 2: Deduplicate on category_id
        #
        # Why: the generator could theoretically write the same
        # category twice (e.g. if run_generator.py is re-run).
        # We keep the first occurrence and drop duplicates.
        # keep="first" is consistent and deterministic.
        # ------------------------------------------------------
        before_dedup = len(df)
        df = df.drop_duplicates(subset=["category_id"], keep="first")
        after_dedup = len(df)

        dupes_dropped = before_dedup - after_dedup
        if dupes_dropped > 0:
            logger.warning(f"Duplicates dropped: {dupes_dropped}")

        # ------------------------------------------------------
        # STEP 3: Validate — category_id and category_name
        # must never be null. Rows that fail go to rejected_records.
        #
        # Why these two columns?
        # category_id   — it's the business key, referenced by dim_product
        # category_name — required by the warehouse NOT NULL constraint
        # ------------------------------------------------------
        null_mask = df["category_id"].isna() | df["category_name"].isna()
        rejected_df = df[null_mask]
        df          = df[~null_mask].reset_index(drop=True)

        rows_rejected = len(rejected_df)

        # Log each rejected row into audit.rejected_records
        for _, row in rejected_df.iterrows():
            auditor.log_rejected_record(
                record_id=str(row.get("category_id", "UNKNOWN")),
                rejection_reason="null category_id or category_name",
                raw_data=row.to_dict()
            )

        # Log the validation check result into audit.data_quality_checks
        auditor.log_quality_check(
            check_name="no_null_category_id_or_name",
            rows_checked=before_dedup,
            rows_failed=rows_rejected
        )

        logger.info(
            f"Validation complete | "
            f"passed={len(df)} rejected={rows_rejected}"
        )

        # ------------------------------------------------------
        # STEP 4: Select Silver columns
        #
        # We drop: event_type, ingested_at
        # These are Bronze metadata — not needed in Silver.
        # parent_category_id is kept as-is (business key).
        # The warehouse loader will resolve it to parent_category_sk.
        # ------------------------------------------------------
        df = df[["category_id", "category_name", "parent_category_id"]]

        # ------------------------------------------------------
        # STEP 5: Write to Silver as Parquet
        #
        # Why Parquet?
        # - Columnar format — much faster for analytics queries
        # - Compressed — dim_category is small but habit matters
        # - Typed — preserves dtypes unlike CSV
        #
        # mkdir parents=True means the folder is created if it
        # doesn't exist yet — safe to run on a fresh environment.
        # ------------------------------------------------------
        OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(OUTPUT_PATH, index=False)

        rows_written = len(df)
        logger.info(f"Silver Parquet written: {OUTPUT_PATH} | rows={rows_written}")

        # ------------------------------------------------------
        # STEP 6: Tell the auditor the final row counts.
        # These are flushed to audit.pipeline_runs on end().
        # ------------------------------------------------------
        auditor.set_row_counts(
            rows_read=rows_read,
            rows_written=rows_written,
            rows_rejected=rows_rejected
        )

        logger.info(
            f"silver_dim_category complete | "
            f"read={rows_read} written={rows_written} rejected={rows_rejected}"
        )


# ------------------------------------------------------------
# Allows running directly: python -m etl.transform.silver.silver_dim_category
# ------------------------------------------------------------
if __name__ == "__main__":
    run()
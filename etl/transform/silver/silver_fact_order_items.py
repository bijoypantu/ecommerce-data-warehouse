# etl/transform/silver/silver_fact_order_items.py
# ============================================================
# Silver transform for fact_order_items.
#
# Bronze  →  data_lake/raw/fact_order_items.jsonl
# Silver  →  data_lake/processed/fact_order_items.parquet
#
# Responsibilities:
#   1. Read Bronze JSONL via read_bronze()
#   2. Deduplicate on order_item_id for latest event per order
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

SILVER_PATH = Path("data_lake/processed/fact_order_items.parquet")

def run():
    with PipelineAuditor(
        pipeline_name="silver_fact_order_items",
        table_name="fact_order_items",
        layer="silver"
    ) as auditor:
        
        # ------------------------------------------------------
        # STEP 1: Read Bronze
        # ------------------------------------------------------
        df = read_bronze("fact_order_items")
        rows_read = len(df)
        logger.info(f"Rows read from Bronze: {rows_read}")

        # ------------------------------------------------------
        # STEP 2: Deduplicate on order_item_id — keep latest event per order
        # ------------------------------------------------------
        before_dedup = rows_read
        df = df.sort_values("order_last_updated_at", ascending=False)
        df = df.drop_duplicates(subset=["order_item_id"], keep="first")
        after_dedup = len(df)

        dupes_dropped = before_dedup - after_dedup
        if (dupes_dropped > 0):
            logger.warning(f"Duplicates dropped: {dupes_dropped}")

        # ------------------------------------------------------
        # STEP 3: Validate — NOT NULL columns 
        #                   and check constrains
        # ------------------------------------------------------
        null_mask = df["order_item_id"].isna() | df["customer_id"].isna() | df["order_created_at"].isna() | df["order_last_updated_at"].isna() \
                    | df["order_status"].isna() | df["order_channel"].isna() | df["total_order_amount"].isna() | df["order_discount_total"].isna() \
                    | df["currency_code"].isna()
        status_mask = ~df["order_status"].str.strip().str.lower().isin(["created", "processing", "cancelled", "shipped", "delivered"])
        channel_mask = ~df["order_channel"].str.strip().str.lower().isin(["web", "mobile", "marketplace"])

        reject_mask = null_mask | status_mask | channel_mask
        rejected_df = df[reject_mask]
        df = df[~reject_mask].reset_index(drop=True)

        rows_rejected = len(rejected_df)

        for _, row in rejected_df.iterrows():
            auditor.log_rejected_record(
                record_id=str(row.get("order_item_id", "UNKNOWN")),
                rejection_reason= "null values or incorrect status or channel",
                raw_data=row.to_dict()
            )

        auditor.log_quality_check(
            check_name="null_values_or_incorrect_status_or_channel",
            rows_checked=before_dedup,
            rows_failed=rows_rejected
        )

        logger.info(
            f"Validation complete | "
            f"passed={len(df)} rejected={rows_rejected}"
        )
        df["date_sk"] = df["order_created_at"].dt.strftime("%Y%m%d").astype(int)

        # ------------------------------------------------------
        # STEP 4: Select Silver columns
        # ------------------------------------------------------
        df = df[[
            "order_item_id", "customer_id", "date_sk", "order_created_at",
            "order_last_updated_at", "order_status", "order_channel",
            "total_order_amount", "order_discount_total", "currency_code"
        ]]

        # ------------------------------------------------------
        # STEP 5: Write to Silver as Parquet
        # ------------------------------------------------------
        SILVER_PATH.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(SILVER_PATH, index=False)

        rows_written = len(df)
        logger.info(f"Silver Parquet written: {SILVER_PATH} | rows={rows_written}")

        # ------------------------------------------------------
        # STEP 6: Tell the auditor the final row counts.
        # ------------------------------------------------------
        auditor.set_row_counts(
            rows_read=rows_read,
            rows_written=rows_written,
            rows_rejected=rows_rejected
        )

        logger.info(
            f"silver_fact_order_items complete |"
            f"read={rows_read} written={rows_written} rejected={rows_rejected}"
        )


if __name__ == "__main__":
    run()
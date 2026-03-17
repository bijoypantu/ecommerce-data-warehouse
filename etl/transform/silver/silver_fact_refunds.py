# etl/transform/silver/silver_fact_refunds.py
# ============================================================
# Silver transform for fact_refunds.
#
# Bronze  →  data_lake/raw/fact_refunds.jsonl
# Silver  →  data_lake/processed/fact_refunds.parquet
#
# Responsibilities:
#   1. Read Bronze JSONL via read_bronze()
#   2. Deduplicate — keep latest event per order_item_id, refund_id
#   3. Validate — nulls, check constraints, date ranges
#   4. Derive refund_date_sk from initiated_at
#   5. Select only Silver-relevant columns
#   6. Write clean Parquet to Silver layer
#   7. Track everything via PipelineAuditor
# ============================================================

from pathlib import Path
import pandas as pd

from etl.extract.read_bronze import read_bronze
from etl.utils.logger import get_logger
from etl.utils.auditor import PipelineAuditor

logger = get_logger(__name__)


PROJECT_ROOT = Path(__file__).resolve().parents[3]

# NOT NULL columns — validated in bulk
NOT_NULL_COLS = [
    "refund_id", "order_id", "order_item_id", "customer_id",
    "initiated_at", "refund_quantity", "refund_amount",
    "refund_reason", "refund_status", "currency_code"
]

VALID_STATUSES  = ["initiated", "approved", "processed", "rejected"]


def run():
    with PipelineAuditor(
        pipeline_name="silver_fact_refunds",
        table_name="fact_refunds",
        layer="silver"
    ) as auditor:

        # ------------------------------------------------------
        # STEP 1: Read Bronze
        # ------------------------------------------------------
        try:
            df, execution_date = read_bronze("fact_refunds")
            if df.empty:
                logger.info("No refund records for this date — skipping")
                return
        except FileNotFoundError:
            logger.info("fact_refunds.jsonl not found for this date — skipping")
            return
        rows_read = len(df)
        logger.info(f"Rows read from Bronze: {rows_read}")
        
        OUTPUT_PATH = PROJECT_ROOT / "data_lake" / "processed" / execution_date / "fact_refunds.parquet"

        # ------------------------------------------------------
        # STEP 2: Deduplicate
        # ------------------------------------------------------
        before_dedup = rows_read
        df = df.sort_values("initiated_at").drop_duplicates(subset=["order_item_id", "refund_id"], keep="last")
        after_dedup = len(df)

        dupes_dropped = before_dedup - after_dedup
        if dupes_dropped > 0:
            logger.warning(f"Duplicates dropped: {dupes_dropped}")

        # ------------------------------------------------------
        # STEP 3: Validate
        #
        # Three categories of checks:
        #   a) NOT NULL constraints
        #   b) CHECK constraints
        # ------------------------------------------------------

        # a) NOT NULL
        null_mask = pd.Series(False, index=df.index)
        for col in NOT_NULL_COLS:
            null_mask |= df[col].isna()

        # b) Valid values
        status_mask  = ~df["refund_status"].str.strip().str.lower().isin(VALID_STATUSES)

        # c) Amount and date range rules
        quantity_mask = df["refund_quantity"] <= 0
        amount_mask   = df["refund_amount"] <= 0
        date_range_mask = (
            df["processed_at"].notna() & 
            (df["processed_at"] < df["initiated_at"])
        )
        status_criteria_mask = (
            (df["refund_status"] == "processed") & (df["processed_at"].isna())
        ) | (
            (~df["refund_status"].isin(["processed", "rejected"])) & (df["processed_at"].notna())
        )

        reject_mask = null_mask | status_mask | amount_mask | quantity_mask | date_range_mask | status_criteria_mask
        rejected_df = df[reject_mask]
        df          = df[~reject_mask].reset_index(drop=True)

        rows_rejected = len(rejected_df)

        # Log each rejected row with specific reasons
        for _, row in rejected_df.iterrows():
            reasons = []

            for col in NOT_NULL_COLS:
                if pd.isna(row.get(col)):
                    reasons.append(f"null {col}")

            if pd.notna(row.get("refund_status")) and row["refund_status"].strip().lower() not in VALID_STATUSES:
                reasons.append(f"invalid refund_status: {row['refund_status']}")

            if pd.notna(row.get("refund_quantity")) and row["refund_quantity"] <= 0:
                reasons.append("refund_quantity must be greater than 0")
            
            if pd.notna(row.get("refund_amount")) and row["refund_amount"] <= 0:
                reasons.append("refund_amount must be greater than 0")

            if pd.notna(row.get("processed_at")) and pd.notna(row.get("initiated_at")):
                if row["processed_at"] < row["initiated_at"]:
                    reasons.append("processed_at is before initiated_at")

            if row.get("refund_status") == "processed" and pd.isna(row.get("processed_at")):
                reasons.append("processed status requires processed_at")

            if row.get("refund_status") != "processed" and pd.notna(row.get("processed_at")):
                reasons.append("non-processed status cannot have processed_at")

            
            auditor.log_rejected_record(
                record_id=str(row.get("refund_id", "UNKNOWN")),
                rejection_reason=", ".join(reasons) if reasons else "unknown",
                raw_data=row.to_dict()
            )

        auditor.log_quality_check(
            check_name="nulls_status_quantity_amounts",
            rows_checked=before_dedup,
            rows_failed=rows_rejected
        )

        logger.info(f"Validation complete | passed={len(df)} rejected={rows_rejected}")

        # ------------------------------------------------------
        # STEP 4: Derive refund_date_sk
        # ------------------------------------------------------
        df["refund_date_sk"] = df["initiated_at"].dt.strftime("%Y%m%d").astype(int)

        # ------------------------------------------------------
        # STEP 5: Select Silver columns
        # Drop: event_type, ingested_at (Bronze metadata)
        # Drop: refund_amount_inr
        # ------------------------------------------------------
        df = df[[
            "refund_id", "order_id", "order_item_id", "customer_id",
            "refund_date_sk", "initiated_at", "processed_at",
            "refund_quantity", "refund_amount", "refund_reason",
            "refund_status", "currency_code"
        ]]

        # ------------------------------------------------------
        # STEP 6: Writing to Silver as Parquet
        # ------------------------------------------------------
        OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(OUTPUT_PATH, index=False)

        rows_written = len(df)
        logger.info(f"Silver Parquet written: {OUTPUT_PATH} | rows={rows_written}")

        # ------------------------------------------------------
        # STEP 7: Telling the auditor the final row counts
        # ------------------------------------------------------
        auditor.set_row_counts(
            rows_read=rows_read,
            rows_written=rows_written,
            rows_rejected=rows_rejected
        )

        logger.info(
            f"silver_fact_refunds complete | "
            f"read={rows_read} written={rows_written} rejected={rows_rejected}"
        )


if __name__ == "__main__":
    run()
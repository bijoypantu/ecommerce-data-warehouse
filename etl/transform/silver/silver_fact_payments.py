# etl/transform/silver/silver_fact_payments.py
# ============================================================
# Silver transform for fact_payments.
#
# Bronze  →  data_lake/raw/fact_payments.jsonl
# Silver  →  data_lake/processed/fact_payments.parquet
#
# Responsibilities:
#   1. Read Bronze JSONL via read_bronze()
#   2. Deduplicate — keep latest event per order_id, payment_attempt_id
#   3. Validate — nulls, check constraints, date ranges
#   4. Derive date_sk from order_created_at
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

SILVER_PATH = Path("data_lake/processed/fact_payments.parquet")

# NOT NULL columns — validated in bulk
NOT_NULL_COLS = [
    "payment_attempt_id", "order_id", "customer_id", "payment_timestamp",
    "payment_method", "payment_status", "amount", "currency_code"
]

VALID_STATUSES  = ["success", "failed", "pending", "cancelled"]
VALID_METHODS  = ["credit_card", "debit_card", "upi", "net_banking", "cod", "wallet"]


def run():
    with PipelineAuditor(
        pipeline_name="silver_fact_payments",
        table_name="fact_payments",
        layer="silver"
    ) as auditor:

        # ------------------------------------------------------
        # STEP 1: Read Bronze
        # ------------------------------------------------------
        df = read_bronze("fact_payments")
        rows_read = len(df)
        logger.info(f"Rows read from Bronze: {rows_read}")

        # ------------------------------------------------------
        # STEP 2: Deduplicate
        # ------------------------------------------------------
        before_dedup = rows_read
        df = df.drop_duplicates(subset=["order_id", "payment_attempt_id"], keep="first")
        after_dedup = len(df)

        dupes_dropped = before_dedup - after_dedup
        if dupes_dropped > 0:
            logger.warning(f"Duplicates dropped: {dupes_dropped}")

        # ------------------------------------------------------
        # STEP 3: Validate
        #
        # Three categories of checks:
        #   a) NOT NULL constraints
        #   b) CHECK constraint — valid status and channel values
        #   c) CHECK constraints — amount and date range rules
        # ------------------------------------------------------

        # a) NOT NULL
        null_mask = pd.Series(False, index=df.index)
        for col in NOT_NULL_COLS:
            null_mask |= df[col].isna()

        # b) Valid values
        status_mask  = ~df["payment_status"].str.strip().str.lower().isin(VALID_STATUSES)
        method_mask = ~df["payment_method"].str.strip().str.lower().isin(VALID_METHODS)

        # c) Amount and date range rules
        amount_mask   = df["amount"] <= 0

        reject_mask = null_mask | status_mask | amount_mask | method_mask
        rejected_df = df[reject_mask]
        df          = df[~reject_mask].reset_index(drop=True)

        rows_rejected = len(rejected_df)

        # Log each rejected row with specific reasons
        for _, row in rejected_df.iterrows():
            reasons = []

            for col in NOT_NULL_COLS:
                if pd.isna(row.get(col)):
                    reasons.append(f"null {col}")

            if pd.notna(row.get("payment_status")) and row["payment_status"].strip().lower() not in VALID_STATUSES:
                reasons.append(f"invalid payment_status: {row['payment_status']}")

            if pd.notna(row.get("payment_method")) and row["payment_method"].strip().lower() not in VALID_METHODS:
                reasons.append(f"invalid payment_method: {row['payment_method']}")

            if pd.notna(row.get("amount")) and row["amount"] <= 0:
                reasons.append("amount must be greater than 0")

            
            auditor.log_rejected_record(
                record_id=str(row.get("payment_attempt_id", "UNKNOWN")),
                rejection_reason=", ".join(reasons) if reasons else "unknown",
                raw_data=row.to_dict()
            )

        auditor.log_quality_check(
            check_name="nulls_status_method_amounts",
            rows_checked=before_dedup,
            rows_failed=rows_rejected
        )

        logger.info(f"Validation complete | passed={len(df)} rejected={rows_rejected}")

        # ------------------------------------------------------
        # STEP 4: Derive payment_date_sk
        # ------------------------------------------------------
        df["payment_date_sk"] = df["payment_timestamp"].dt.strftime("%Y%m%d").astype(int)

        # ------------------------------------------------------
        # STEP 5: Select Silver columns
        # Drop: event_type, ingested_at (Bronze metadata)
        # Drop: amount_inr
        # ------------------------------------------------------
        df = df[[
            "payment_attempt_id", "order_id", "customer_id", "payment_date_sk",
            "payment_timestamp", "payment_method", "payment_provider",
            "payment_status", "gateway_response_code", "amount",
            "currency_code"
        ]]

        # ------------------------------------------------------
        # STEP 6: Writing to Silver as Parquet
        # ------------------------------------------------------
        SILVER_PATH.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(SILVER_PATH, index=False)

        rows_written = len(df)
        logger.info(f"Silver Parquet written: {SILVER_PATH} | rows={rows_written}")

        # ------------------------------------------------------
        # STEP 7: Telling the auditor the final row counts
        # ------------------------------------------------------
        auditor.set_row_counts(
            rows_read=rows_read,
            rows_written=rows_written,
            rows_rejected=rows_rejected
        )

        logger.info(
            f"silver_fact_payments complete | "
            f"read={rows_read} written={rows_written} rejected={rows_rejected}"
        )


if __name__ == "__main__":
    run()
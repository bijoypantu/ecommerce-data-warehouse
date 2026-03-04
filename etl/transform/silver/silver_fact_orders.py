# etl/transform/silver/silver_fact_orders.py
# ============================================================
# Silver transform for fact_orders.
#
# Bronze  →  data_lake/raw/fact_orders.jsonl
# Silver  →  data_lake/processed/fact_orders.parquet
#
# Responsibilities:
#   1. Read Bronze JSONL via read_bronze()
#   2. Deduplicate — keep latest event per order_id
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

SILVER_PATH = Path("data_lake/processed/fact_orders.parquet")

# NOT NULL columns — validated in bulk
NOT_NULL_COLS = [
    "order_id", "customer_id", "order_created_at", "order_last_updated_at",
    "order_status", "order_channel", "total_order_amount",
    "order_discount_total", "currency_code"
]

VALID_STATUSES  = ["created", "processing", "cancelled", "shipped", "delivered"]
VALID_CHANNELS  = ["web", "mobile", "marketplace"]


def run():
    with PipelineAuditor(
        pipeline_name="silver_fact_orders",
        table_name="fact_orders",
        layer="silver"
    ) as auditor:

        # ------------------------------------------------------
        # STEP 1: Read Bronze
        # fact_orders.jsonl contains 6 mixed event types.
        # We read all of them — dedup resolves to final state.
        # ------------------------------------------------------
        df = read_bronze("fact_orders")
        rows_read = len(df)
        logger.info(f"Rows read from Bronze: {rows_read}")

        # ------------------------------------------------------
        # STEP 2: Deduplicate — keep latest event per order_id
        #
        # Sort descending by order_last_updated_at so the most
        # recent event per order floats to the top, then keep
        # the first occurrence after grouping by order_id.
        # ------------------------------------------------------
        before_dedup = rows_read
        df = df.sort_values("order_last_updated_at", ascending=False)
        df = df.drop_duplicates(subset=["order_id"], keep="first")
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
        status_mask  = ~df["order_status"].str.strip().str.lower().isin(VALID_STATUSES)
        channel_mask = ~df["order_channel"].str.strip().str.lower().isin(VALID_CHANNELS)

        # c) Amount and date range rules
        amount_mask   = df["total_order_amount"] < 0
        date_mask     = df["order_last_updated_at"] < df["order_created_at"]
        discount_mask = (
            (df["order_discount_total"] < 0) |
            (df["order_discount_total"] > df["total_order_amount"])
        )

        reject_mask = null_mask | status_mask | channel_mask | amount_mask | date_mask | discount_mask
        rejected_df = df[reject_mask]
        df          = df[~reject_mask].reset_index(drop=True)

        rows_rejected = len(rejected_df)

        # Log each rejected row with specific reasons
        for _, row in rejected_df.iterrows():
            reasons = []

            for col in NOT_NULL_COLS:
                if pd.isna(row.get(col)):
                    reasons.append(f"null {col}")

            if pd.notna(row.get("order_status")) and row["order_status"].strip().lower() not in VALID_STATUSES:
                reasons.append(f"invalid order_status: {row['order_status']}")

            if pd.notna(row.get("order_channel")) and row["order_channel"].strip().lower() not in VALID_CHANNELS:
                reasons.append(f"invalid order_channel: {row['order_channel']}")

            if pd.notna(row.get("total_order_amount")) and row["total_order_amount"] < 0:
                reasons.append("negative total_order_amount")

            if pd.notna(row.get("order_last_updated_at")) and pd.notna(row.get("order_created_at")):
                if row["order_last_updated_at"] < row["order_created_at"]:
                    reasons.append("order_last_updated_at before order_created_at")

            if pd.notna(row.get("order_discount_total")):
                if row["order_discount_total"] < 0:
                    reasons.append("negative order_discount_total")
                if pd.notna(row.get("total_order_amount")) and row["order_discount_total"] > row["total_order_amount"]:
                    reasons.append("discount exceeds total_order_amount")

            auditor.log_rejected_record(
                record_id=str(row.get("order_id", "UNKNOWN")),
                rejection_reason=", ".join(reasons) if reasons else "unknown",
                raw_data=row.to_dict()
            )

        auditor.log_quality_check(
            check_name="nulls_status_channel_amounts_date_range",
            rows_checked=before_dedup,
            rows_failed=rows_rejected
        )

        logger.info(f"Validation complete | passed={len(df)} rejected={rows_rejected}")

        # ------------------------------------------------------
        # STEP 4: Derive date_sk
        #
        # date_sk is a derived business key — not a DB surrogate.
        # Calculated here in Silver so all downstream layers
        # have it ready without recalculating.
        # Format: YYYYMMDD integer e.g. 20240712
        # ------------------------------------------------------
        df["date_sk"] = df["order_created_at"].dt.strftime("%Y%m%d").astype(int)

        # ------------------------------------------------------
        # STEP 5: Select Silver columns
        # Drop: event_type, ingested_at (Bronze metadata)
        # Drop: total_order_amount_inr, order_discount_total_inr
        #       (currency conversion happens in Gold)
        # ------------------------------------------------------
        df = df[[
            "order_id", "customer_id", "date_sk",
            "order_created_at", "order_last_updated_at",
            "order_status", "order_channel",
            "total_order_amount", "order_discount_total",
            "currency_code"
        ]]

        # ------------------------------------------------------
        # STEP 6: Write to Silver as Parquet
        # ------------------------------------------------------
        SILVER_PATH.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(SILVER_PATH, index=False)

        rows_written = len(df)
        logger.info(f"Silver Parquet written: {SILVER_PATH} | rows={rows_written}")

        # ------------------------------------------------------
        # STEP 7: Tell the auditor the final row counts
        # ------------------------------------------------------
        auditor.set_row_counts(
            rows_read=rows_read,
            rows_written=rows_written,
            rows_rejected=rows_rejected
        )

        logger.info(
            f"silver_fact_orders complete | "
            f"read={rows_read} written={rows_written} rejected={rows_rejected}"
        )


if __name__ == "__main__":
    run()
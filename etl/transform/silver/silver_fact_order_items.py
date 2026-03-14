# etl/transform/silver/silver_fact_order_items.py
# ============================================================
# Silver transform for fact_order_items.
#
# Bronze  →  data_lake/raw/fact_order_items.jsonl
# Silver  →  data_lake/processed/fact_order_items.parquet
#
# Responsibilities:
#   1. Read Bronze JSONL via read_bronze()
#   2. Deduplicate — keep latest event per order_item_id
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


PROJECT_ROOT = Path(__file__).resolve().parents[3]

# NOT NULL columns — validated in bulk
NOT_NULL_COLS = [
    "order_item_id", "order_id", "product_id", "customer_id",
    "order_created_at", "quantity", "unit_price_at_order",
    "discount_amount", "line_total_amount"
]


def run():
    with PipelineAuditor(
        pipeline_name="silver_fact_order_items",
        table_name="fact_order_items",
        layer="silver"
    ) as auditor:

        # ------------------------------------------------------
        # STEP 1: Read Bronze
        # fact_order_items.jsonl contains a single event type: order_item_created.
        # Dedup guards against duplicate writes only.
        # ------------------------------------------------------
        try:
            df, execution_date = read_bronze("fact_order_items")
        except FileNotFoundError:
            logger.info("fact_order_items.jsonl not found for this date — skipping")
            return
        rows_read = len(df)
        logger.info(f"Rows read from Bronze: {rows_read}")
        
        OUTPUT_PATH = PROJECT_ROOT / "data_lake" / "processed" / execution_date / "fact_order_items.parquet"

        # ------------------------------------------------------
        # STEP 2: Deduplicate — keep latest event per order_item_id
        #
        # Sort descending by order_last_updated_at so the most
        # recent event per order floats to the top, then keep
        # the first occurrence after grouping by order_item_id.
        # ------------------------------------------------------
        before_dedup = rows_read
        df = df.drop_duplicates(subset=["order_id", "order_item_id"], keep="first")
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

        # b) Amount and date range rules
        quantity_mask   = df["quantity"] <= 0
        unit_price_mask = df["unit_price_at_order"] < 0
        line_amnt_mask = df["line_total_amount"] < 0
        
        discount_mask = (
            (df["discount_amount"] < 0) |
            (df["discount_amount"] > df["total_amount"])
        )

        reject_mask = null_mask | quantity_mask | unit_price_mask | line_amnt_mask | discount_mask
        rejected_df = df[reject_mask]
        df          = df[~reject_mask].reset_index(drop=True)

        rows_rejected = len(rejected_df)

        # Log each rejected row with specific reasons
        for _, row in rejected_df.iterrows():
            reasons = []

            for col in NOT_NULL_COLS:
                if pd.isna(row.get(col)):
                    reasons.append(f"null {col}")

            if pd.notna(row.get("quantity")) and row["quantity"] <= 0:
                reasons.append("invalid order_item_quantity")
            if pd.notna(row.get("unit_price_at_order")) and row["unit_price_at_order"] < 0:
                reasons.append("negative unit_price_at_order")
            if pd.notna(row.get("line_total_amount")) and row["line_total_amount"] < 0:
                reasons.append("negative line_total_amount")

            if pd.notna(row.get("discount_amount")):
                if row["discount_amount"] < 0:
                    reasons.append("negative order_discount_total")
                if pd.notna(row.get("total_amount")) and row["discount_amount"] > row["total_amount"]:
                    reasons.append("discount exceeds total_item_amount")

            auditor.log_rejected_record(
                record_id=str(row.get("order_item_id", "UNKNOWN")),
                rejection_reason=", ".join(reasons) if reasons else "unknown",
                raw_data=row.to_dict()
            )

        auditor.log_quality_check(
            check_name="nulls_amounts_discounts",
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
        # STEP 5: Selecting Silver columns
        # Drop: total_amount, category_name (Bronze-internal columns)
        # Drop: event_type, ingested_at (Bronze metadata)
        # line_total_amount_inr calculated in Gold
        # ------------------------------------------------------
        df = df[[
            "order_item_id", "order_id", "product_id", "customer_id",
            "date_sk", "quantity", "unit_price_at_order",
            "discount_amount", "line_total_amount"
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
            f"silver_fact_order_items complete | "
            f"read={rows_read} written={rows_written} rejected={rows_rejected}"
        )


if __name__ == "__main__":
    run()
# etl/transform/silver/silver_fact_shipments.py
# ============================================================
# Silver transform for fact_shipments.
#
# Bronze  →  data_lake/raw/fact_shipments.jsonl
# Silver  →  data_lake/processed/fact_shipments.parquet
#
# Responsibilities:
#   1. Read Bronze JSONL via read_bronze()
#   2. Deduplicate — keep latest event per order_id, shipment_id
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

SILVER_PATH = Path("data_lake/processed/fact_shipments.parquet")

# NOT NULL columns — validated in bulk
NOT_NULL_COLS = [
    "shipment_id", "order_id", "order_item_id", "customer_id",
    "shipped_at", "shipment_status", "carrier", "shipped_quantity"
]

VALID_STATUSES  = ['shipped','delivered','failed']


def run():
    with PipelineAuditor(
        pipeline_name="silver_fact_shipments",
        table_name="fact_shipments",
        layer="silver"
    ) as auditor:

        # ------------------------------------------------------
        # STEP 1: Read Bronze
        # ------------------------------------------------------
        df = read_bronze("fact_shipments")
        rows_read = len(df)
        logger.info(f"Rows read from Bronze: {rows_read}")

        # ------------------------------------------------------
        # STEP 2: Deduplicate
        # ------------------------------------------------------
        before_dedup = rows_read
        df = df.drop_duplicates(subset=["order_item_id", "shipment_id"], keep="first")
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
        status_mask  = ~df["shipment_status"].str.strip().str.lower().isin(VALID_STATUSES)

        # c) Amount and date range rules
        quantity_mask   = df["shipped_quantity"] <= 0
        date_range_mask = (
            df["delivered_at"].notna() & 
            (df["delivered_at"] < df["shipped_at"])
        )
        delivered_mask = (
            (df["shipment_status"] == "delivered") & 
            (df["delivered_at"].isna())
        ) | (
            (df["shipment_status"] != "delivered") & 
            (df["delivered_at"].notna())
        )

        reject_mask = null_mask | status_mask | quantity_mask | date_range_mask | delivered_mask
        rejected_df = df[reject_mask]
        df          = df[~reject_mask].reset_index(drop=True)

        rows_rejected = len(rejected_df)

        # Log each rejected row with specific reasons
        for _, row in rejected_df.iterrows():
            reasons = []

            for col in NOT_NULL_COLS:
                if pd.isna(row.get(col)):
                    reasons.append(f"null {col}")

            if pd.notna(row.get("shipment_status")) and row["shipment_status"].strip().lower() not in VALID_STATUSES:
                reasons.append(f"invalid shipment_status: {row['shipment_status']}")

            if pd.notna(row.get("shipped_quantity")) and row["shipped_quantity"] <= 0:
                reasons.append("shipped_quantity must be greater than 0")

            if pd.notna(row.get("delivered_at")) and pd.notna(row.get("shipped_at")):
                if row["delivered_at"] < row["shipped_at"]:
                    reasons.append("delivered_at is before shipped_at")

            if row.get("shipment_status") == "delivered" and pd.isna(row.get("delivered_at")):
                reasons.append("delivered status requires delivered_at")

            if row.get("shipment_status") != "delivered" and pd.notna(row.get("delivered_at")):
                reasons.append("non-delivered status cannot have delivered_at")
            
            
            auditor.log_rejected_record(
                record_id=str(row.get("shipment_id", "UNKNOWN")),
                rejection_reason=", ".join(reasons) if reasons else "unknown",
                raw_data=row.to_dict()
            )

        auditor.log_quality_check(
            check_name="nulls_status_quantity_date_ranges",
            rows_checked=before_dedup,
            rows_failed=rows_rejected
        )

        logger.info(f"Validation complete | passed={len(df)} rejected={rows_rejected}")

        # ------------------------------------------------------
        # STEP 4: Derive shipment_date_sk and delivery_date_sk
        # ------------------------------------------------------
        df["shipment_date_sk"] = df["shipped_at"].dt.strftime("%Y%m%d").astype(int)
        df["delivery_date_sk"] = df["delivered_at"].apply(
            lambda x: int(pd.Timestamp(x).strftime("%Y%m%d")) if pd.notna(x) else None
        )

        # ------------------------------------------------------
        # STEP 5: Select Silver columns
        # Drop: event_type, ingested_at (Bronze metadata)
        # ------------------------------------------------------
        df = df[[
            "shipment_id", "order_id", "order_item_id", "customer_id",
            "shipment_date_sk", "delivery_date_sk", "shipped_at",
            "delivered_at", "shipment_status", "carrier", "tracking_id",
            "shipped_quantity"
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
            f"silver_fact_shipments complete | "
            f"read={rows_read} written={rows_written} rejected={rows_rejected}"
        )


if __name__ == "__main__":
    run()
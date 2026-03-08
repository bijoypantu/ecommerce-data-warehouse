# etl/transform/gold/gold_fact_customer_segment_snapshot.py
# ============================================================
# Gold transform for fact_customer_segment_snapshot.
#
# Gold (fact_orders) → data_lake/curated/fact_customer_segment_snapshot.parquet
#
# Responsibilities:
#   1. Read Gold fact_orders — delivered orders only
#   2. Generate one snapshot per month-end date in the data range
#   3. For each snapshot month — calculate LTM (last 12 months) metrics
#      per customer: revenue_ltm_inr, order_count_ltm
#   4. Calculate revenue and frequency percentiles per snapshot
#   5. Calculate composite_score = 0.7 * revenue_pct + 0.3 * freq_pct
#   6. Apply data-driven segment labels using 80th/30th percentile cutoffs
#   7. Write to Gold curated layer as Parquet
#   8. Track everything via PipelineAuditor
# ============================================================

from pathlib import Path
import pandas as pd

from etl.utils.logger import get_logger
from etl.utils.auditor import PipelineAuditor

logger = get_logger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[3]
GOLD_ORDERS_PATH  = PROJECT_ROOT / "data_lake" / "curated" / "fact_orders.parquet"
OUTPUT_PATH = PROJECT_ROOT / "data_lake" / "curated" / "fact_customer_segment_snapshot.parquet"


def run():
    with PipelineAuditor(
        pipeline_name="gold_fact_customer_segment_snapshot",
        table_name="fact_customer_segment_snapshot",
        layer="gold"
    ) as auditor:

        # ------------------------------------------------------
        # STEP 1: Read Gold fact_orders
        # Filter to delivered orders only — segment snapshots
        # are based on realized revenue, not pending orders.
        # ------------------------------------------------------
        orders_df = pd.read_parquet(GOLD_ORDERS_PATH)
        rows_read = len(orders_df)
        logger.info(f"Rows read from Gold fact_orders: {rows_read}")

        delivered_df = orders_df[orders_df["order_status"] == "delivered"].copy()
        logger.info(f"Delivered orders filtered: {len(delivered_df)}")

        # Convert date_sk (int YYYYMMDD) to actual date for window calculations
        delivered_df["order_date"] = pd.to_datetime(
            delivered_df["date_sk"].astype(str), format="%Y%m%d"
        )

        # ------------------------------------------------------
        # STEP 2: Generate snapshot month-end dates
        #
        # pd.date_range with freq="ME" generates the last day of
        # every month between start and end automatically.
        # Example: 2021-01-31, 2021-02-28, 2021-03-31 ...
        #
        # We use the actual data range so snapshots are grounded
        # in real order dates, not hardcoded boundaries.
        # ------------------------------------------------------
        min_date = delivered_df["order_date"].min()
        max_date = delivered_df["order_date"].max()

        months = pd.date_range(
            start=min_date.to_period("M").to_timestamp("M"),
            end=max_date.to_period("M").to_timestamp("M"),
            freq="ME"
        )
        logger.info(f"Snapshot months to process: {len(months)} ({months[0].date()} → {months[-1].date()})")

        # ------------------------------------------------------
        # STEP 3: Build LTM metrics per customer per snapshot month
        #
        # For each month-end M:
        #   - LTM window = (M - 12 months) to M  (exclusive start)
        #   - Filter delivered orders within that window
        #   - Group by customer_id → sum revenue, count orders
        #   - Calculate percentiles within this snapshot's population
        #
        # Percentiles are calculated per snapshot month so each
        # month's rankings reflect that month's active customers.
        # A customer inactive for 12 months won't appear at all.
        # ------------------------------------------------------
        snapshots = []

        for month_end in months:
            window_start = month_end - pd.DateOffset(months=12)

            window_df = delivered_df[
                (delivered_df["order_date"] > window_start) &
                (delivered_df["order_date"] <= month_end)
            ]

            if window_df.empty:
                continue

            # Aggregate per customer for this LTM window
            ltm = window_df.groupby("customer_id").agg(
                revenue_ltm_inr=("total_order_amount_inr", "sum"),
                order_count_ltm=("order_id", "count")
            ).reset_index()

            # ------------------------------------------------------
            # Percentile rankings — done per snapshot month so each
            # month's rankings are independent of other months.
            # rank(pct=True) returns values between 0 and 1.
            # A customer at 0.8 is in the 80th percentile.
            # ------------------------------------------------------
            ltm["revenue_percentile"]   = ltm["revenue_ltm_inr"].rank(pct=True).round(5)
            ltm["frequency_percentile"] = ltm["order_count_ltm"].rank(pct=True).round(5)

            # Composite score — revenue weighted more heavily than frequency
            # because a high-spending customer is more valuable than a
            # frequent low-spending one.
            ltm["composite_score"] = (
                0.7 * ltm["revenue_percentile"] +
                0.3 * ltm["frequency_percentile"]
            ).round(5)

            # Tag with snapshot month — last day of the month
            ltm["snapshot_month"] = month_end.date()

            snapshots.append(ltm)

        # ------------------------------------------------------
        # STEP 4: Combine all snapshot months into one DataFrame
        # ------------------------------------------------------
        df = pd.concat(snapshots, ignore_index=True)
        logger.info(f"Total snapshot rows generated: {len(df)}")

        # ------------------------------------------------------
        # STEP 5: Apply data-driven segment labels
        #
        # Thresholds are derived from the actual composite score
        # distribution across ALL snapshots — not hardcoded values.
        # This means segments adapt to your data automatically.
        #
        #   composite_score >= 80th percentile → 'high'
        #   composite_score <  30th percentile → 'low'
        #   everything in between              → 'medium'
        # ------------------------------------------------------
        high_cutoff = df["composite_score"].quantile(0.8)
        low_cutoff  = df["composite_score"].quantile(0.3)

        logger.info(f"Segment cutoffs | high >= {high_cutoff:.4f} | low < {low_cutoff:.4f}")

        df["segment_label"] = "medium"
        df.loc[df["composite_score"] >= high_cutoff, "segment_label"] = "high"
        df.loc[df["composite_score"] <  low_cutoff,  "segment_label"] = "low"

        # Log segment distribution — useful for sanity checking
        high_count   = (df["segment_label"] == "high").sum()
        medium_count = (df["segment_label"] == "medium").sum()
        low_count    = (df["segment_label"] == "low").sum()
        logger.info(
            f"Segment distribution | "
            f"high={high_count} medium={medium_count} low={low_count}"
        )

        # ------------------------------------------------------
        # STEP 6: Select final columns matching warehouse DDL
        # Drop customer_id — warehouse uses customer_sk (resolved
        # by loader). Keep it for now as loader will need it
        # to resolve customer_sk.
        # ------------------------------------------------------
        df = df[[
            "snapshot_month", "customer_id",
            "revenue_ltm_inr", "order_count_ltm",
            "revenue_percentile", "frequency_percentile",
            "composite_score", "segment_label"
        ]]

        # ------------------------------------------------------
        # STEP 7: Write to Gold curated layer
        # ------------------------------------------------------
        OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(OUTPUT_PATH, index=False)

        rows_written = len(df)
        logger.info(f"Gold Parquet written: {OUTPUT_PATH} | rows={rows_written}")

        # ------------------------------------------------------
        # STEP 8: Tell the auditor the final row counts
        # ------------------------------------------------------
        auditor.set_row_counts(
            rows_read=rows_read,
            rows_written=rows_written,
            rows_rejected=0
        )

        logger.info(
            f"gold_fact_customer_segment_snapshot complete | "
            f"read={rows_read} written={rows_written}"
        )


if __name__ == "__main__":
    run()
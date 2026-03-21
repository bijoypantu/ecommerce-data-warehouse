# etl/transform/gold/gold_fact_customer_segment_snapshot.py
# ============================================================
# Gold transform for fact_customer_segment_snapshot.
#
# Warehouse (fact_orders) → data_lake/curated/YYYY-MM-DD/fact_customer_segment_snapshot.parquet
#
# Responsibilities:
#   1. Check if today is month-end — skip if not
#   2. Query ALL delivered orders from dw.fact_orders (full history)
#   3. Generate one snapshot per month-end date in the data range
#   4. For each snapshot month — calculate LTM (last 12 months) metrics
#      per customer: revenue_ltm_inr, order_count_ltm
#   5. Calculate revenue and frequency percentiles per snapshot
#   6. Calculate composite_score = 0.7 * revenue_pct + 0.3 * freq_pct
#   7. Apply data-driven segment labels using 80th/30th percentile cutoffs
#   8. Write to Gold curated layer as Parquet
#   9. Track everything via PipelineAuditor
# ============================================================

from pathlib import Path
import pandas as pd
import calendar
from datetime import date

from etl.utils.logger import get_logger
from etl.utils.auditor import PipelineAuditor, _get_connection

logger = get_logger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[3]


def run():
    # ----------------------------------------------------------
    # STEP 1: Check if today is month-end
    # Scan latest Gold partition to determine generation date
    # ----------------------------------------------------------
    partitions = sorted((PROJECT_ROOT / "data_lake" / "curated").glob("????-??-??"))
    if not partitions:
        logger.info("No Gold partitions found — skipping")
        return

    today = date.fromisoformat(partitions[-1].name)
    last_day = calendar.monthrange(today.year, today.month)[1]

    if today.day != last_day:
        logger.info(f"Not end of month ({today}) — skipping")
        return

    execution_date = today.isoformat()
    OUTPUT_PATH = PROJECT_ROOT / "data_lake" / "curated" / execution_date / "fact_customer_segment_snapshot.parquet"

    with PipelineAuditor(
        pipeline_name="gold_fact_customer_segment_snapshot",
        table_name="fact_customer_segment_snapshot",
        layer="gold"
    ) as auditor:

        # ------------------------------------------------------
        # STEP 2: Query ALL delivered orders from warehouse
        # We need full history for LTM calculations — not just
        # today's partition. This is why we query the warehouse
        # directly instead of reading Gold parquet.
        # ------------------------------------------------------
        conn = _get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT
                        fo.order_id,
                        dc.customer_id,
                        fo.date_sk,
                        fo.total_order_amount_inr,
                        fo.order_last_updated_at
                    FROM dw.fact_orders fo
                    JOIN dw.dim_customer dc ON fo.customer_sk = dc.customer_sk
                    WHERE fo.order_status = 'delivered'
                    AND fo.total_order_amount_inr IS NOT NULL
                """)
                rows = cur.fetchall()
        finally:
            conn.close()

        if not rows:
            logger.info("No delivered orders in warehouse — skipping snapshot")
            return

        orders_df = pd.DataFrame(rows, columns=[
            "order_id", "customer_id", "date_sk",
            "total_order_amount_inr", "order_last_updated_at"
        ])

        rows_read = len(orders_df)
        logger.info(f"Delivered orders read from warehouse: {rows_read}")

        # ------------------------------------------------------
        # STEP 3: Convert date_sk to actual date
        # ------------------------------------------------------
        orders_df["order_date"] = pd.to_datetime(
            orders_df["date_sk"].astype(str), format="%Y%m%d"
        )

        # ------------------------------------------------------
        # STEP 4: Generate snapshot month-end dates
        # ------------------------------------------------------
        min_date = orders_df["order_date"].min()
        max_date = orders_df["order_date"].max()

        months = pd.date_range(
            start=min_date.to_period("M").to_timestamp("M"),
            end=max_date.to_period("M").to_timestamp("M"),
            freq="ME"
        )
        logger.info(f"Snapshot months to process: {len(months)} ({months[0].date()} → {months[-1].date()})")

        # ------------------------------------------------------
        # STEP 5: Build LTM metrics per customer per snapshot month
        # ------------------------------------------------------
        snapshots = []

        for month_end in months:
            window_start = month_end - pd.DateOffset(months=12)

            window_df = orders_df[
                (orders_df["order_date"] > window_start) &
                (orders_df["order_date"] <= month_end)
            ]

            if window_df.empty:
                continue

            ltm = window_df.groupby("customer_id").agg(
                revenue_ltm_inr=("total_order_amount_inr", "sum"),
                order_count_ltm=("order_id", "count")
            ).reset_index()

            ltm["revenue_percentile"]   = ltm["revenue_ltm_inr"].rank(pct=True).round(5)
            ltm["frequency_percentile"] = ltm["order_count_ltm"].rank(pct=True).round(5)

            ltm["composite_score"] = (
                0.7 * ltm["revenue_percentile"] +
                0.3 * ltm["frequency_percentile"]
            ).round(5)

            ltm["snapshot_month"] = month_end.date()
            snapshots.append(ltm)

        if not snapshots:
            logger.info("No snapshots generated — skipping")
            return

        # ------------------------------------------------------
        # STEP 6: Combine all snapshot months
        # ------------------------------------------------------
        df = pd.concat(snapshots, ignore_index=True)
        logger.info(f"Total snapshot rows generated: {len(df)}")

        # ------------------------------------------------------
        # STEP 7: Apply segment labels
        # ------------------------------------------------------
        high_cutoff = df["composite_score"].quantile(0.8)
        low_cutoff  = df["composite_score"].quantile(0.3)

        logger.info(f"Segment cutoffs | high >= {high_cutoff:.4f} | low < {low_cutoff:.4f}")

        df["segment_label"] = "medium"
        df.loc[df["composite_score"] >= high_cutoff, "segment_label"] = "high"
        df.loc[df["composite_score"] <  low_cutoff,  "segment_label"] = "low"

        high_count   = (df["segment_label"] == "high").sum()
        medium_count = (df["segment_label"] == "medium").sum()
        low_count    = (df["segment_label"] == "low").sum()
        logger.info(f"Segment distribution | high={high_count} medium={medium_count} low={low_count}")

        # ------------------------------------------------------
        # STEP 8: Select final columns
        # ------------------------------------------------------
        df = df[[
            "snapshot_month", "customer_id",
            "revenue_ltm_inr", "order_count_ltm",
            "revenue_percentile", "frequency_percentile",
            "composite_score", "segment_label"
        ]]

        # ------------------------------------------------------
        # STEP 9: Write to Gold curated layer
        # ------------------------------------------------------
        OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(OUTPUT_PATH, index=False)

        rows_written = len(df)
        logger.info(f"Gold Parquet written: {OUTPUT_PATH} | rows={rows_written}")

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
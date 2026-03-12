from pathlib import Path
import pandas as pd

from etl.extract.read_silver import read_silver
from etl.utils.logger import get_logger
from etl.utils.auditor import PipelineAuditor

logger = get_logger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[3]
RATES_PATH = PROJECT_ROOT / "warehouse" / "seeds" / "all_currencies_to_inr.csv"


def run():
    with PipelineAuditor(
        pipeline_name="gold_fact_orders",
        table_name="fact_orders",
        layer="gold"
    ) as auditor:
        
        # Read Silver Parquet
        df, execution_date = read_silver("fact_orders")
        rows_read = len(df)
        logger.info(f"Rows read from Silver: {rows_read}")
        OUTPUT_PATH = PROJECT_ROOT / "data_lake" / "curated" / execution_date / "fact_orders.parquet"

        # Read exchange rates
        rates_df = pd.read_csv(RATES_PATH)

        df = df.merge(
            rates_df[["date_sk", "currency_code", "rate_to_inr"]],
            on=["date_sk", "currency_code"],
            how="left"
        )
        
        # INR rate is always 1.0 — fill missing INR rates explicitly
        df.loc[df["currency_code"] == "INR", "rate_to_inr"] = \
        df.loc[df["currency_code"] == "INR", "rate_to_inr"].fillna(1.0)

        # After merge, rows with no rate match will have rate_to_inr = NaN
        missing_rate_mask = df["rate_to_inr"].isna()
        rows_rejected = missing_rate_mask.sum()
        # Log rejected rows to audit.rejected_records
        if missing_rate_mask.sum() > 0:
            for _, row in df[missing_rate_mask].iterrows():
                auditor.log_rejected_record(
                    record_id=str(row.get("order_id", "UNKNOWN")),
                    rejection_reason=f"missing exchange rate for {row['currency_code']} on {row['date_sk']}",
                    raw_data=row.to_dict()
                )

        # NaN * anything = NaN — so _inr columns are automatically NULL for missing rates
        df["total_order_amount_inr"]   = (df["total_order_amount"] * df["rate_to_inr"]).round(2)
        df["order_discount_total_inr"] = (df["order_discount_total"] * df["rate_to_inr"]).round(2)
        
        df = df.drop(columns=["rate_to_inr"])

        OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(OUTPUT_PATH, index=False)

        rows_written = len(df)
        logger.info(f"Gold Parquet written: {OUTPUT_PATH} | rows={rows_written}")

        auditor.set_row_counts(
            rows_read=rows_read,
            rows_written=rows_written,
            rows_rejected=rows_rejected
        )

        logger.info(
            f"gold_fact_orders complete | "
            f"read={rows_read} written={rows_written} rejected={rows_rejected}"
        )


if __name__ == "__main__":
    run()
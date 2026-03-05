from pathlib import Path
import pandas as pd

from etl.utils.logger import get_logger
from etl.utils.auditor import PipelineAuditor

logger = get_logger(__name__)

SILVER_PATH = Path("data_lake/processed/fact_refunds.parquet")
GOLD_PATH   = Path("data_lake/curated/fact_refunds.parquet")
RATES_PATH  = Path("warehouse/seeds/all_currencies_to_inr.csv")


def run():
    with PipelineAuditor(
        pipeline_name="gold_fact_refunds",
        table_name="fact_refunds",
        layer="gold"
    ) as auditor:
        
        # Read Silver Parquet
        df = pd.read_parquet(SILVER_PATH)
        rows_read = len(df)
        logger.info(f"Rows read from Silver: {rows_read}")

        # Read exchange rates
        rates_df = pd.read_csv(RATES_PATH)

        rates_df = rates_df.rename(columns={"date_sk": "refund_date_sk"})
        df = df.merge(
            rates_df[["refund_date_sk", "currency_code", "rate_to_inr"]],
            on=["refund_date_sk", "currency_code"],
            how="left"
        )

        # INR rate is always 1.0 — fill missing INR rates explicitly
        df.loc[df["currency_code"] == "INR", "rate_to_inr"] = \
        df.loc[df["currency_code"] == "INR", "rate_to_inr"].fillna(1.0)

        # After merge, rows with no rate match will have rate_to_inr = NaN
        missing_rate_mask = df["rate_to_inr"].isna()
        rows_rejected = missing_rate_mask.sum()
        if missing_rate_mask.sum() > 0:
            logger.warning(f"Missing exchange rates: {missing_rate_mask.sum()} rows — _inr will be NULL")

        # NaN * anything = NaN — so _inr columns are automatically NULL for missing rates
        df["refund_amount_inr"] = (df["refund_amount"] * df["rate_to_inr"]).round(2)
        
        df = df.drop(columns=["rate_to_inr"])

        GOLD_PATH.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(GOLD_PATH, index=False)

        rows_written = len(df)
        logger.info(f"Gold Parquet written: {GOLD_PATH} | rows={rows_written}")

        auditor.set_row_counts(
            rows_read=rows_read,
            rows_written=rows_written,
            rows_rejected=rows_rejected
        )

        logger.info(
            f"gold_fact_refunds complete | "
            f"read={rows_read} written={rows_written} rejected={rows_rejected}"
        )


if __name__ == "__main__":
    run()
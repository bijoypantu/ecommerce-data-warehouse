# etl/extract/generator/run_generator.py
# ============================================================
# Entry point to run raw data generators.
# Currently runs category raw generator only.
# ============================================================

from datetime import datetime
from .gen_categories_raw import generate_categories_raw
from .gen_products_raw import generate_products_raw
from .gen_customers_raw import generate_customers_raw


def run_all_generators(run_date: datetime):
    """
    Calls all raw generators.
    Each generator writes to:
    data_lake/raw/<run_date>/
    """

    print("\n===================================")
    print(" Running RAW Data Generators ")
    print("===================================")

    # -------------------------------------------------
    # Categories (Master Data)
    # -------------------------------------------------
    generate_categories_raw(run_date)

    # -------------------------------------------------
    # Future generators (uncomment later)
    # -------------------------------------------------
    generate_products_raw(run_date)
    generate_customers_raw(run_date)
    # generate_orders_raw(run_date)

    print("\n All raw generators completed successfully.")


if __name__ == "__main__":
    run_date = datetime.utcnow()
    run_all_generators(run_date)
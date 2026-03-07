# etl/load/run_loader.py
# ============================================================
# Master warehouse loader — runs all loaders in dependency order.
#
# Load order:
#   1. dim_category         (no dependencies)
#   2. dim_customer         (no dependencies)
#   3. dim_product          (depends on dim_category)
#   4. fact_orders          (depends on dim_customer)
#   5. fact_order_items     (depends on fact_orders, dim_product)
#   6. fact_payments        (depends on fact_orders)
#   7. fact_shipments       (depends on fact_order_items)
#   8. fact_refunds         (depends on fact_order_items)
#   9. fact_customer_segment_snapshot  (depends on dim_customer)
# ============================================================

from etl.utils.logger import get_logger
from etl.utils.auditor import _get_connection

from etl.load.load_dim_category import run as cat_run
from etl.load.load_dim_customer import run as cust_run
from etl.load.load_dim_product import run as prod_run
from etl.load.load_fact_orders import run as ord_run
from etl.load.load_fact_order_items import run as item_run
from etl.load.load_fact_payments import run as pay_run
from etl.load.load_fact_shipments import run as ship_run
from etl.load.load_fact_refunds import run as ref_run
from etl.load.load_customer_segment_snapshot import run as seg_run

logger = get_logger(__name__)

LOADERS = [
    ("dim_category",                    cat_run),
    ("dim_customer",                    cust_run),
    ("dim_product",                     prod_run),
    ("fact_orders",                     ord_run),
    ("fact_order_items",                item_run),
    ("fact_payments",                   pay_run),
    ("fact_shipments",                  ship_run),
    ("fact_refunds",                    ref_run),
    ("fact_customer_segment_snapshot",  seg_run),
]

if __name__ == "__main__":
    conn = _get_connection()
    try:
        logger.info("=== Warehouse load started ===")
        for table_name, loader in LOADERS:
            logger.info(f"Loading {table_name}...")
            loader(conn)
            logger.info(f"Finished {table_name}.")
        logger.info("=== Warehouse load complete ===")
    except Exception:
        logger.exception("Warehouse load failed")
        raise
    finally:
        conn.close()
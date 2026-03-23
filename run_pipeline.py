import subprocess
import time

PIPELINE_STEPS = [
    "python -m data_generator.run_generator",

    "python -m etl.transform.silver.silver_dim_category",
    "python -m etl.transform.silver.silver_dim_customer",
    "python -m etl.transform.silver.silver_dim_product",
    "python -m etl.transform.silver.silver_fact_orders",
    "python -m etl.transform.silver.silver_fact_order_items",
    "python -m etl.transform.silver.silver_fact_payments",
    "python -m etl.transform.silver.silver_fact_shipments",
    "python -m etl.transform.silver.silver_fact_refunds",

    "python -m etl.transform.gold.gold_fact_orders",
    "python -m etl.transform.gold.gold_fact_order_items",
    "python -m etl.transform.gold.gold_fact_payments",
    "python -m etl.transform.gold.gold_fact_refunds",
    "python -m etl.transform.gold.gold_fact_customer_segment_snapshot",

    "python -m etl.load.run_loader"
]

def run_pipeline():
    for step in PIPELINE_STEPS:
        start = time.time()
        print(f"Running: {step}")
        result = subprocess.run(step, shell=True)
        print(f"Completed in {time.time() - start:.1f}s")

        if result.returncode != 0:
            raise Exception(f"Pipeline failed at step {step} on run {i+1}")

if __name__ == "__main__":
    for i in range(365):
        print(f"\n===== RUN {i+1} =====\n")
        
        run_pipeline()

        # small delay between runs (in seconds)
        time.sleep(2)
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def run_gold_fact_orders():
    import sys
    sys.path.insert(0, "/opt/airflow/project")
    from etl.transform.gold.gold_fact_orders import run
    run()

def run_gold_fact_order_items():
    import sys
    sys.path.insert(0, "/opt/airflow/project")
    from etl.transform.gold.gold_fact_order_items import run
    run()

def run_gold_fact_payments():
    import sys
    sys.path.insert(0, "/opt/airflow/project")
    from etl.transform.gold.gold_fact_payments import run
    run()

def run_gold_fact_refunds():
    import sys
    sys.path.insert(0, "/opt/airflow/project")
    from etl.transform.gold.gold_fact_refunds import run
    run()

def run_gold_fact_customer_segment_snapshot():
    import sys
    sys.path.insert(0, "/opt/airflow/project")
    from etl.transform.gold.gold_fact_customer_segment_snapshot import run
    run()

# Define the DAG
with DAG(
    dag_id="gold_etl_dag",
    start_date=datetime(2026, 1, 1),
    schedule_interval="@weekly",
    catchup=False
) as dag:

    # Define the tasks
    task_orders = PythonOperator(
        task_id="gold_fact_orders",
        python_callable=run_gold_fact_orders
    )

    task_order_items = PythonOperator(
        task_id="gold_fact_order_items",
        python_callable=run_gold_fact_order_items
    )

    task_payments = PythonOperator(
        task_id = "gold_fact_payments",
        python_callable=run_gold_fact_payments
    )

    task_refunds = PythonOperator(
        task_id="gold_fact_refunds",
        python_callable=run_gold_fact_refunds
    )

    task_cust_segments = PythonOperator(
        task_id="gold_fact_customer_segment_snapshot",
        python_callable=run_gold_fact_customer_segment_snapshot
    )

    # 3. Define dependencies
    task_orders >> task_order_items
    task_orders >> task_cust_segments
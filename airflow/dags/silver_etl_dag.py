from airflow import DAG
from airflow.operators.python import PythonOperator #type: ignore
from airflow.operators.trigger_dagrun import TriggerDagRunOperator #type: ignore
from datetime import datetime


def run_silver_dim_category():
    import sys
    sys.path.insert(0, "/opt/airflow/project")
    from etl.transform.silver.silver_dim_category import run
    run()

def run_silver_dim_customer():
    import sys
    sys.path.insert(0, "/opt/airflow/project")
    from etl.transform.silver.silver_dim_customer import run
    run()

def run_silver_dim_product():
    import sys
    sys.path.insert(0, "/opt/airflow/project")
    from etl.transform.silver.silver_dim_product import run
    run()

def run_silver_fact_orders():
    import sys
    sys.path.insert(0, "/opt/airflow/project")
    from etl.transform.silver.silver_fact_orders import run
    run()

def run_silver_fact_order_items():
    import sys
    sys.path.insert(0, "/opt/airflow/project")
    from etl.transform.silver.silver_fact_order_items import run
    run()

def run_silver_fact_payments():
    import sys
    sys.path.insert(0, "/opt/airflow/project")
    from etl.transform.silver.silver_fact_payments import run
    run()

def run_silver_fact_shipments():
    import sys
    sys.path.insert(0, "/opt/airflow/project")
    from etl.transform.silver.silver_fact_shipments import run
    run()

def run_silver_fact_refunds():
    import sys
    sys.path.insert(0, "/opt/airflow/project")
    from etl.transform.silver.silver_fact_refunds import run
    run()

# Define the DAG
with DAG(
    dag_id="silver_etl_dag",
    start_date=datetime(2026, 3, 12),
    schedule_interval = None,
    catchup=False
) as dag:
    
    # Define the tasks
    task_category = PythonOperator(
        task_id="silver_dim_category",
        python_callable=run_silver_dim_category
    )

    task_customer = PythonOperator(
        task_id="silver_dim_customer",
        python_callable=run_silver_dim_customer
    )

    task_product = PythonOperator(
        task_id = "silver_dim_product",
        python_callable=run_silver_dim_product
    )

    task_orders = PythonOperator(
        task_id="silver_fact_orders",
        python_callable=run_silver_fact_orders
    )

    task_order_items = PythonOperator(
        task_id="silver_fact_order_items",
        python_callable=run_silver_fact_order_items
    )

    task_payments = PythonOperator(
        task_id = "silver_fact_payments",
        python_callable=run_silver_fact_payments
    )

    task_shipments = PythonOperator(
        task_id="silver_fact_shipments",
        python_callable=run_silver_fact_shipments
    )

    task_refunds = PythonOperator(
        task_id="silver_fact_refunds",
        python_callable=run_silver_fact_refunds
    )

    trigger_gold = TriggerDagRunOperator(
        task_id="trigger_gold_etl",
        trigger_dag_id="gold_etl_dag",
        wait_for_completion=False
    )

    # 3. Define dependencies
    task_category >> task_product
    task_customer >> task_orders

    task_product >> task_order_items
    task_orders >> task_order_items
    task_orders >> task_payments

    task_order_items >> task_shipments
    task_order_items >> task_refunds

    task_shipments >> trigger_gold
    task_refunds >> trigger_gold
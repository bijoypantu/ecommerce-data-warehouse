from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

import sys
sys.path.insert(0, "/opt/airflow/project")

from etl.utils.auditor import _get_connection

def run_silver_dim_category():
    from etl.transform.silver.silver_dim_category import run
    conn = _get_connection()
    try:
        run(conn)
    finally:
        conn.close()

def run_silver_dim_customer():
    from etl.transform.silver.silver_dim_customer import run
    conn = _get_connection()
    try:
        run(conn)
    finally:
        conn.close()

def run_silver_dim_product():
    from etl.transform.silver.silver_dim_product import run
    conn = _get_connection()
    try:
        run(conn)
    finally:
        conn.close()

def run_silver_fact_orders():
    from etl.transform.silver.silver_fact_orders import run
    conn = _get_connection()
    try:
        run(conn)
    finally:
        conn.close()

def run_silver_fact_order_items():
    from etl.transform.silver.silver_fact_order_items import run
    conn = _get_connection()
    try:
        run(conn)
    finally:
        conn.close()

def run_silver_fact_payments():
    from etl.transform.silver.silver_fact_payments import run
    conn = _get_connection()
    try:
        run(conn)
    finally:
        conn.close()

def run_silver_fact_shipments():
    from etl.transform.silver.silver_fact_shipments import run
    conn = _get_connection()
    try:
        run(conn)
    finally:
        conn.close()

def run_silver_fact_refunds():
    from etl.transform.silver.silver_fact_refunds import run
    conn = _get_connection()
    try:
        run(conn)
    finally:
        conn.close()

# Define the DAG
with DAG(
    dag_id="silver_etl_dag",
    start_date=datetime(2026, 3, 10),
    schedule_interval="@weekly",
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

    task_order = PythonOperator(
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

    # 3. Define dependencies
    task_category >> task_product
    task_customer >> task_order

    task_product >> task_order_items
    task_order >> task_order_items
    task_order >> task_payments

    task_order_items >> task_shipments
    task_order_items >> task_refunds
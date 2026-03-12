from airflow import DAG
from airflow.operators.python import PythonOperator #type: ignore
from airflow.operators.trigger_dagrun import TriggerDagRunOperator #type: ignore
from datetime import datetime


def run_data_generator():
    import sys
    sys.path.insert(0, "/opt/airflow/project")
    from data_generator.run_generator import generate
    generate()

# Define the DAG
with DAG(
    dag_id="data_generator_dag",
    start_date=datetime(2026, 3, 12),
    schedule_interval="30 14 * * *",  # 8:00 PM IST = 14:30 UTC
    catchup=False
) as dag:
    
    
    task_generator = PythonOperator(
        task_id="run_generator",
        python_callable=run_data_generator
    )

    trigger_silver = TriggerDagRunOperator(
        task_id="trigger_silver_etl",
        trigger_dag_id="silver_etl_dag",
        wait_for_completion=False
    )

    task_generator >> trigger_silver
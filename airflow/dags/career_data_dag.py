from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
from etl_professionals import run_etl_pipeline

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'career_data_pipeline',
    default_args=default_args,
    description='Data pipeline for professional career data',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2025, 3, 8),
    catchup=True,
)

def execute_etl(**kwargs):
    """Execute the ETL pipeline with automatic incremental detection"""
    input_file = 'professionals_nested.json'
    output_db = 'professionals_dimensional.db'
    
    # Check if this is first run or incremental
    is_incremental = os.path.exists(output_db)
    
    return run_etl_pipeline(input_file, output_db, incremental=is_incremental)

run_etl_task = PythonOperator(
    task_id='run_etl_pipeline',
    python_callable=execute_etl,
    dag=dag,
) 
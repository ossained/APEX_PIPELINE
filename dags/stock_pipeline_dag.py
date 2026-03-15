from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from stock_pipeline import run_pipeline

with DAG(
    dag_id="stock_prices_pipeline",
    start_date=datetime(2024, 3, 15),
    schedule="*/30 * * * *",
    catchup=False,
    tags=["stockprices", "Apex", "etl"],
) as dag:
    
    t1 = PythonOperator(
        task_id="run_pipeline",
        python_callable=run_pipeline,
    )

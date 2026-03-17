from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from stock_pipeline import run_pipeline
from stock_pipeline import validate

with DAG(
    dag_id="stock_prices_pipeline",
    start_date=datetime(2024, 3, 15),
    schedule="*/10 * * * *",
    catchup=False,
    tags=["stockprices", "Apex", "etl"],
) as dag:
    
    t1 = PythonOperator(
        task_id="run_pipeline",
        python_callable=run_pipeline,
    )

    t2 = PythonOperator(
        task_id="validate_data",
        python_callable=lambda ti: validate(ti.xcom_pull(task_ids="run_pipeline"))
    )

    
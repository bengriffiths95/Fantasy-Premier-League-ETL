import os
from dotenv import load_dotenv
from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from scripts.extract import extract_data


with DAG(
    "etl_dag",
    start_date=datetime(2024, 12, 19),
    description="DAG to orchestrate ETL process",
    schedule="@daily",
    catchup=False,
):

    load_dotenv()
    team_id = os.environ["FPL_TID"]
    extract_bucket_name = os.environ["s3_extract_bucket_name"]

    extract = PythonOperator(
        task_id="extract_task",
        python_callable=extract_data,
        op_kwargs={"team_id": team_id, "bucket_name": extract_bucket_name},
    )

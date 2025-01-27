import os
from dotenv import load_dotenv
from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from scripts.extract import extract_data
from scripts.transform import transform_data
from scripts.load import load_data


with DAG(
    "etl_dag",
    start_date=datetime(2025, 1, 21),
    description="DAG to orchestrate ETL process",
    schedule="5 8 * * *",
    catchup=False,
):

    load_dotenv()
    extract_bucket_name = os.environ["s3_extract_bucket_name"]
    transform_bucket_name = os.environ["s3_transform_bucket_name"]
    rds_user = os.environ["rds_user"]
    rds_password = os.environ["rds_password"]
    rds_host = os.environ["rds_host"]
    rds_port = os.environ["rds_port"]
    rds_db_name = os.environ["rds_db_name"]

    extract = PythonOperator(
        task_id="extract_task",
        python_callable=extract_data,
        op_kwargs={"bucket_name": extract_bucket_name},
    )

    transform = PythonOperator(
        task_id="transform_task",
        python_callable=transform_data,
        op_kwargs={
            "source_bucket": extract_bucket_name,
            "destination_bucket": transform_bucket_name,
        },
    )

    load = PythonOperator(
        task_id="load_task",
        python_callable=load_data,
        op_kwargs={
            "rds_user": rds_user,
            "rds_password": rds_password,
            "rds_host": rds_host,
            "rds_port": rds_port,
            "rds_db_name": rds_db_name,
            "bucket_name": transform_bucket_name,
        },
    )

    extract >> transform >> load

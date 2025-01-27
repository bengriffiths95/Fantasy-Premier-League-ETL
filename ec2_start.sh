#!/usr/bin/bash
set -x
export AIRFLOW_HOME=/home/ubuntu/Fantasy-Premier-League-ETL/airflow_home
cd Fantasy-Premier-League-ETL/
source venv/bin/activate
export PYTHONPATH=$(pwd)
airflow webserver &
airflow scheduler
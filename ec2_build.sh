#!/bin/bash
set -e

echo "Create venv"
if [ ! -d "Fantasy-Premier-League-ETL" ]; then
    echo "Error: Directory Fantasy-Premier-League-ETL does not exist."
    exit 1
fi
cd Fantasy-Premier-League-ETL/
python3 -m venv venv
source venv/bin/activate

echo "Install libpq-dev"
sudo apt-get install libpq-dev -y

echo "Install Airflow"
pip install "apache-airflow[postgres]==2.5.0" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.5.0/constraints-3.7.txt"

echo "Set AIRFLOW_HOME to correct directory"
export AIRFLOW_HOME=~/Fantasy-Premier-League-ETL/airflow_home

echo "Initialise Airflow DB, set up Postgres backend" 
airflow db init
sudo apt-get install postgresql postgresql-contrib -y
sudo -i -u postgres psql <<EOF
CREATE DATABASE airflow;
CREATE USER airflow WITH PASSWORD 'airflow';
GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;
EOF
cd airflow/
sed -i 's#sqlite:////home/ubuntu/Fantasy-Premier-League-ETL/airflow/airflow.db#postgresql+psycopg2://airflow:airflow@localhost/airflow#g' airflow.cfg
sed -i 's#SequentialExecutor#LocalExecutor#g' airflow.cfg

echo "Set up Airflow user"
cd ../
airflow db init
airflow users create -u airflow -f airflow -l airflow -r Admin -e airflow@gmail.com
echo "Please enter account password"

echo "Install other dependencies"
pip install -r requirements.txt
import io
import pandas as pd
import boto3
from sqlalchemy import create_engine

try:
    from scripts.helpers import generate_filename
except ImportError:
    from airflow_home.dags.scripts.helpers import generate_filename


def load_data(rds_user, rds_password, rds_host, rds_port, rds_db_name, bucket_name):
    """
    Executes full Load process, invoking create_db_conn, retrieve_s3_parquet & insert_df_into_db

    Parameters:
        rds_user (str): RDS username
        rds_password (str): RDS password
        rds_host (str): RDS hostname
        rds_port (str): RDS port
        rds_db_name (str): RDS database name
        bucket_name (str): Name of the source S3 bucket

    Returns:
        Nothing

    Side Effects:
        On success - parquet files fetched from S3 bucket and inserted into SQL tables
    """
    conn = create_db_conn(rds_user, rds_password, rds_host, rds_port, rds_db_name)

    tables = ["fact_players", "dim_players", "dim_teams", "dim_fixtures"]

    for table in tables:
        file_path = f"{generate_filename(table)}.parquet"
        df = retrieve_s3_parquet(bucket_name, file_path)
        insert_df_into_db(df, conn, table)


def retrieve_s3_parquet(bucket_name, file_name):
    """
    Retrieves Parquet file from s3 bucket and converts it to Pandas dataFrame

    Parameters:
        bucket_name (str): Name of the source S3 bucket
        file_name (str):  Name of the file (key)

    Returns:
        dataFrame: Parquet file converted into a dataFrame object

    Side Effects:
        On failure - error message logged
    """
    try:
        s3 = boto3.client("s3")
        response = s3.get_object(
            Bucket=bucket_name,
            Key=file_name,
        )
        df = pd.read_parquet(io.BytesIO(response["Body"].read()))
        return df
    except Exception as e:
        print(f"retrieve_s3_parquet Error: ", {e})
        raise


def create_db_conn(rds_user, rds_password, rds_host, rds_port, rds_db_name):
    """
    Creates a SQLAlchemy MySQL database connection

    Parameters:
        rds_user (str): RDS username
        rds_password (str): RDS password
        rds_host (str): RDS hostname
        rds_port (str): RDS port
        rds_db_name (str): RDS database name

    Returns:
        SQLAlchemy Engine: db connection for provided credentials

    Side Effects:
        On failure - error message logged
    """
    try:
        conn_str = f"mysql+pymysql://{rds_user}:{rds_password}@{rds_host}:{rds_port}/{rds_db_name}"
        engine = create_engine(conn_str)
        with engine.connect() as conn:
            print("Database connection successful!")
        return engine
    except Exception as e:
        print(f"create_db_conn Error: ", {e})


def insert_df_into_db(df, engine, table_name):
    """
    Insert data into an SQL table

    Parameters:
        df (dataFrame): dataFrame to be inserted into SQL table
        engine (str): SQLAlchemy connection object
        table_name (str): Name of target SQL Table

    Returns:
        Nothing

    Side Effects:
        On success - SQL table updated, success message logged
        On failure - error message logged
    """
    try:
        table_column_names_str = ", ".join(list(df))
        values_placeholders = "%s, " * len(list(df))
        values_placeholders_strip = values_placeholders[:-2]
        values_list = [tuple(x) for x in df.values.tolist()]
        sql = f"INSERT INTO {table_name} ({table_column_names_str}) VALUES ({values_placeholders_strip})"

        conn = engine.raw_connection()
        cursor = conn.cursor()
        cursor.execute(f"TRUNCATE TABLE {table_name};")
        cursor.executemany(sql, values_list)
        conn.commit()
        print(f"{table_name} updated!")
    except Exception as e:
        print(f"insert_df_into_db Error, Table {table_name}: ", {e})
    finally:
        conn.close()

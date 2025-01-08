import io
import pandas as pd
import boto3
from sqlalchemy import create_engine
from scripts.helpers import generate_filename

def load_data(rds_user, rds_password, rds_host, rds_port, bucket_name):

    conn = create_db_conn(rds_user, rds_password, rds_host, rds_port)

    tables = ["fact_players",
              "dim_players",
              "dim_teams",
              "dim_fixtures"]

    for table in tables:
        file_path = f'{generate_filename(table)}.parquet'
        df = retrieve_s3_parquet(bucket_name, file_path)
        insert_df_into_db(df, conn, table)

def retrieve_s3_parquet(bucket_name, file_name):
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

def create_db_conn(rds_user, rds_password, rds_host, rds_port):
    try:
        conn_str = f'mysql+mysqlconnector://{rds_user}:{rds_password}@{rds_host}:{rds_port}'
        engine = create_engine(conn_str)
        print(engine)
        return engine
    except Exception as e:
        print(f'create_db_conn Error: ', {e})

def insert_df_into_db(df, conn, table_name):
    df.to_sql(name=table_name, con=conn, if_exists='replace')
    conn.close()



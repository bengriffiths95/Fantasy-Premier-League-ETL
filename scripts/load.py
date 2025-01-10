import io
import pandas as pd
import boto3
from sqlalchemy import create_engine
from scripts.helpers import generate_filename


def load_data(rds_user, rds_password, rds_host, rds_port, rds_db_name, bucket_name):

    conn = create_db_conn(rds_user, rds_password, rds_host, rds_port, rds_db_name)

    tables = ["fact_players", "dim_players", "dim_teams", "dim_fixtures"]

    for table in tables:
        file_path = f"{generate_filename(table)}.parquet"
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


def create_db_conn(rds_user, rds_password, rds_host, rds_port, rds_db_name):
    try:
        conn_str = f"mysql+pymysql://{rds_user}:{rds_password}@{rds_host}:{rds_port}/{rds_db_name}"
        engine = create_engine(conn_str)
        with engine.connect() as conn:
            print("Database connection successful!")
        return engine
    except Exception as e:
        print(f"create_db_conn Error: ", {e})


def insert_df_into_db(df, engine, table_name):
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
        print(f"insert_df_into_db Error: ", {e})
    finally:
        conn.close()


def seed_db(engine):
    with engine.connect() as conn:
        conn.execute(
            f"DROP TABLE IF EXISTS fact_players, dim_players, dim_teams, dim_fixtures"
        )
        conn.execute(
            f"CREATE TABLE fact_players ( player_id int, team_id int, gameweek_id int )"
        )
        conn.execute(
            f"CREATE TABLE dim_players ( first_name varchar(255), second_name varchar(255), web_name varchar(255), player_id int, team_id int )"
        )
        conn.execute(
            f"CREATE TABLE dim_teams ( team_id int, team_name varchar(255), team_name_short varchar(255) )"
        )
        conn.execute(
            f"CREATE TABLE dim_fixtures ( fixture_id int, gameweek_id int, fixture_date DATE, fixture_time TIME, match_finished bool, home_team_id int, away_team_id int, home_team_score int, away_team_score int, home_team_difficulty int, away_team_difficulty int )"
        )

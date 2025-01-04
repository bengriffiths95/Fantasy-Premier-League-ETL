import json
from datetime import datetime
import boto3
import awswrangler as wr
import pandas as pd


def retrieve_s3_json(bucket_name, file_name):
    try:
        s3 = boto3.client("s3")
        response = s3.get_object(
            Bucket=bucket_name,
            Key=file_name,
        )
        response = json.loads(response["Body"].read().decode("utf-8"))
        return response
    except Exception as e:
        print("Error: ", e)
        raise


def save_df_to_parquet_s3(table_name, table_df, bucket):
    """
    Save a dataframe to s3 bucket

    This function retrieves a desired table_name, dataframe, and s3 bucket name, converts the dataframe to parquet file format and uploads this file to the bucket using awswrangler.

    Parameters:
        table_name (str): The name of the table which you want to use in the s3 key
        table_df (DataFrame): The DataFrame object which you want to convert to parquet
        bucket (str): The name of the S3 bucket you want to upload the file to

    Returns:
        Nothing

    Side Effects:
        On success - Parquet file added to S3 bucket
        On failure - Exception printed to console

    Example:
        >>> convert_dataframe_to_parquet('example_table', dataframe, 'example-bucket')
        Print - Saved to s3://{bucket-name/timestamp/table-name}.parquet
    """

    try:
        # creates current timestamp for s3 file name
        current_timestamp = datetime.now().strftime("%Y-%m-%d")

        # set the desired location for the parquet file
        path = f"s3://{bucket}/{current_timestamp}/{table_name}.parquet"

        # convert the DataFrame to parquet and save to path in s3 bucket
        output = wr.s3.to_parquet(table_df, path)

        print("Added to bucket: ", output)
    except Exception as e:
        print(f"Error processing {table_name}: {e}")


def transform_fact_players(bucket_name):
    try:
        current_timestamp = datetime.now().strftime("%Y-%m-%d")

        # retrieve JSON files from S3 bucket
        bootstrap_static_list = retrieve_s3_json(
            bucket_name, f"2025-01-03/bootstrap-static.json"
        )

        # transform lists into DataFrames
        bs_elements_df = pd.DataFrame(bootstrap_static_list["elements"])
        bs_events_df = pd.DataFrame(bootstrap_static_list["events"])

        # create temp DataFrames with required columns
        temp_gw_df = bs_events_df[["id"]].rename(columns={"id": "gameweek_id"})
        temp_bs_df = bs_elements_df[["id", "team_code"]].rename(
            columns={"id": "player_id", "team_code": "team_id"}
        )

        # merge temp DataFrames
        fact_players_df = pd.merge(temp_bs_df, temp_gw_df, how="cross")
        fact_players_df.index.name = "entry_id"

        return fact_players_df

    except KeyError as e:
        raise KeyError(f"Missing required columns: {e}")

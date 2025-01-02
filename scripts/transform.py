import json
from datetime import datetime
import boto3
import awswrangler as wr


def retrieve_s3_json(bucket_name, file_name):
    try:
        s3 = boto3.client("s3")
        response = s3.get_object(
            Bucket=bucket_name,
            Key=file_name,
        )
        response = json.loads(response["Body"].read().decode("utf-8"))
        print(response)
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

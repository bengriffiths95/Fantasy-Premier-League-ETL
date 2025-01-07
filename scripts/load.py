import io
import pandas as pd
import boto3


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

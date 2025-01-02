import json
import boto3


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

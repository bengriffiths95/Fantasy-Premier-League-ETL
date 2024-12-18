import requests, json
import boto3
from botocore.exceptions import ClientError
from datetime import datetime


def retrieve_data(endpoint):
    try:
        base_url = "https://fantasy.premierleague.com/api/"
        response = requests.get(base_url + endpoint, timeout=5)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as http_err:
        print("Error:", http_err)


def save_json_to_s3(body, bucket, filename):
    try:
        s3 = boto3.client("s3")
        s3.put_object(Body=json.dumps(body), Bucket=bucket, Key=filename)
        return f"Success: {filename} added to bucket {bucket}"
    except ClientError as e:
        print("Error: ", e)
        raise


retrieve_data("fixtures/")

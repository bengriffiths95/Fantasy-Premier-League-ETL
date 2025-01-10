import requests
import json
import boto3
from botocore.exceptions import ClientError
from scripts.helpers import generate_filename


def extract_data(bucket_name):
    endpoints_list = generate_endpoints()

    for endpoint in endpoints_list:
        data = retrieve_data(endpoint)
        filename = f"{generate_filename(endpoint)}.json"
        save_json_to_s3(data, bucket_name, filename)


def generate_endpoints():
    target_endpoints = [
        f"fixtures",
        f"bootstrap-static",
    ]
    gw_endpoints = ["event/{}/live"]

    endpoints_list = list(target_endpoints)
    endpoints_list.extend(gw_endpoints[0].format(i) for i in range(1, 39))
    return endpoints_list


def retrieve_data(endpoint):
    try:
        base_url = "https://fantasy.premierleague.com/api/"
        response = requests.get(base_url + endpoint + "/", timeout=5)
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

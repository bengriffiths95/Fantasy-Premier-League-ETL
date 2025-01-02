import os
import unittest
import pytest
from moto import mock_aws
import boto3
from botocore.exceptions import ClientError
from scripts.transform import retrieve_s3_json


@mock_aws
class TestJSONtoList(unittest.TestCase):
    def setUp(self):
        """Mocked AWS Credentials for moto and test bucket"""
        os.environ["AWS_ACCESS_KEY_ID"] = "testing"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
        os.environ["AWS_SECURITY_TOKEN"] = "testing"
        os.environ["AWS_SESSION_TOKEN"] = "testing"
        os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"

        example_json = '[{"code":2444613,"event":null,"finished":false,"finished_provisional":false,"id":144,"kickoff_time":null,"minutes":0,"provisional_start_time":true,"started":null,"team_a":12,"team_a_score":null,"team_h":8,"team_h_score":null,"stats":[],"team_h_difficulty":5,"team_a_difficulty":3,"pulse_id":115970}]'

        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket="test-bucket")
        s3.put_object(Bucket="test-bucket", Key="test-key", Body=example_json)

    def test_function_returns_list(self):
        output = retrieve_s3_json("test-bucket", "test-key")
        assert isinstance(output, list)

    def test_incorrect_bucket_name_raises_exception(self):
        s3 = boto3.client("s3", region_name="us-east-1")
        test_body = [{"Key": "Value"}]
        with pytest.raises(ClientError) as err:
            retrieve_s3_json("invalid-bucket", "test-key")
        assert (
            str(err.value)
            == "An error occurred (NoSuchBucket) when calling the GetObject operation: The specified bucket does not exist"
        )

    def test_incorrect_file_name_raises_exception(self):
        s3 = boto3.client("s3", region_name="us-east-1")
        test_body = [{"Key": "Value"}]
        with pytest.raises(ClientError) as err:
            retrieve_s3_json("test-bucket", "invalid-key")
        assert (
            str(err.value)
            == "An error occurred (NoSuchKey) when calling the GetObject operation: The specified key does not exist."
        )

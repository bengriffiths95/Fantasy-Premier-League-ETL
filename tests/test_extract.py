import responses
import pytest
import unittest
from moto import mock_aws
import os
import boto3
from botocore.exceptions import ClientError
from datetime import datetime
from python_files.extract import retrieve_data, generate_filename, save_json_to_s3


class TestRetrieveData:
    @responses.activate
    def test_successful_request(self):
        responses.get(
            "https://fantasy.premierleague.com/api/fixtures/",
            json=[{"key": "value"}],
        )
        output = retrieve_data("fixtures/")
        assert output == [{"key": "value"}]

    @responses.activate
    def test_failed_request(self):
        responses.get(
            "https://fantasy.premierleague.com/api/test/",
            body=Exception("404 Not Found"),
        )
        with pytest.raises(Exception) as err:
            retrieve_data("test/")
        assert str(err.value) == "404 Not Found"


class TestGenerateFileName:
    def test_function_returns_correct_filename(self):
        endpoint = "test"
        current_timestamp = datetime.now().strftime("%Y-%m-%d")
        output = generate_filename(endpoint)
        assert output == f"{current_timestamp}-{endpoint}.json"


@mock_aws
class TestSaveJsonToS3(unittest.TestCase):
    def setUp(self):
        """Mocked AWS Credentials for moto and test bucket"""
        os.environ["AWS_ACCESS_KEY_ID"] = "testing"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
        os.environ["AWS_SECURITY_TOKEN"] = "testing"
        os.environ["AWS_SESSION_TOKEN"] = "testing"
        os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"

        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket="test-bucket")

    def test_successful_request_returns_200(self):

        s3 = boto3.client("s3", region_name="us-east-1")
        test_body = [{"Key": "Value"}]
        save_json_to_s3(test_body, "test-bucket", "test-filename")
        output = s3.list_objects_v2(Bucket="test-bucket")
        assert output["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_successful_request_returns_success_msg(self):
        s3 = boto3.client("s3", region_name="us-east-1")
        test_body = [{"Key": "Value"}]
        output = save_json_to_s3(test_body, "test-bucket", "test-filename")
        assert output == "Success: test-filename added to bucket test-bucket"

    def test_successful_request_saves_file_in_json_format(self):
        s3 = boto3.client("s3", region_name="us-east-1")
        test_body = [{"Key": "Value"}]
        save_json_to_s3(test_body, "test-bucket", "test-filename.json")
        output = s3.get_object(Bucket="test-bucket", Key="test-filename.json")
        assert isinstance((output["Body"].read().decode("utf-8")), str)

    def test_incorrect_bucket_name_raises_exception(self):
        s3 = boto3.client("s3", region_name="us-east-1")
        test_body = [{"Key": "Value"}]
        with pytest.raises(ClientError) as err:
            save_json_to_s3(test_body, "incorrect-bucket", "test-filename")
        assert (
            str(err.value)
            == "An error occurred (NoSuchBucket) when calling the PutObject operation: The specified bucket does not exist"
        )

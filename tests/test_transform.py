import os
import unittest
from unittest.mock import patch
import pytest
from moto import mock_aws
import boto3
from botocore.exceptions import ClientError
import pandas as pd
from scripts.transform import retrieve_s3_json, save_df_to_parquet_s3


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

test_df = pd.DataFrame([{'code': 2444613, 'event': None, 'finished': False, 'finished_provisional': False, 'id': 144, 'kickoff_time': None, 'minutes': 0, 'provisional_start_time': True, 'started': None, 'team_a': 12, 'team_a_score': None, 'team_h': 8, 'team_h_score': None, 'stats': [], 'team_h_difficulty': 5, 'team_a_difficulty': 3, 'pulse_id': 115970}])

class TestDfToParquetToS3(unittest.TestCase):
    @patch("scripts.transform.wr.s3.to_parquet")
    def test_function_uploads_file_to_s3_bucket(self, mock_df_to_parquet):
        # mock aws wrangler
        mock_df_to_parquet.return_value = '{"paths": ["s3://test-bucket/2025-01-02 12:52:03/test.parquet"], "partitions_values": []}'

        # invoke function
        save_df_to_parquet_s3("test_table", test_df, "test_bucket")

        # check mock was called correctly
        mock_df_to_parquet.assert_called_once()

    @patch("scripts.transform.wr.s3.to_parquet")
    @patch("builtins.print")
    def test_function_raises_exception_for_invalid_bucket(
        self, mock_print, mock_to_parquet
    ):
        # mock exception
        mock_to_parquet.side_effect = Exception("NoSuchBucket")

        # invoke function
        save_df_to_parquet_s3("test_table", test_df, "test_bucket")

        # assertion
        mock_print.assert_any_call("Error processing test_table: NoSuchBucket")
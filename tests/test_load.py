import os
import io
import unittest
from unittest.mock import patch, MagicMock
import pytest
import boto3
from botocore.exceptions import ClientError
from moto import mock_aws
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from scripts.load import retrieve_s3_parquet


@mock_aws
class TestRetrieveS3Parquet(unittest.TestCase):
    def setUp(self):
        """Mocked AWS Credentials for moto and test bucket"""
        os.environ["AWS_ACCESS_KEY_ID"] = "testing"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
        os.environ["AWS_SECURITY_TOKEN"] = "testing"
        os.environ["AWS_SESSION_TOKEN"] = "testing"
        os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"

        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket="test-bucket")

    @patch("scripts.load.boto3.client")
    def test_function_returns_DataFrame(self, mock_boto3_client):
        example_df = pd.DataFrame({"test1": [1, 2], "test2": [1, 2]})

        buffer = io.BytesIO()
        example_df.to_parquet(buffer, index=False)
        buffer.seek(0)

        mock_s3 = MagicMock()
        mock_boto3_client.return_value = mock_s3
        mock_s3.get_object.return_value = {"Body": io.BytesIO(buffer.read())}

        output = retrieve_s3_parquet("test-bucket", "test-key")
        assert isinstance(output, pd.DataFrame)

    def test_incorrect_file_name_raises_exception(self):
        s3 = boto3.client("s3", region_name="us-east-1")
        test_body = [{"Key": "Value"}]
        with pytest.raises(ClientError) as err:
            retrieve_s3_parquet("test-bucket", "invalid-key")
        assert (
            str(err.value)
            == "An error occurred (NoSuchKey) when calling the GetObject operation: The specified key does not exist."
        )

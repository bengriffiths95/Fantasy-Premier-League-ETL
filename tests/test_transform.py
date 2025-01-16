import os
import unittest
from unittest.mock import patch
from datetime import datetime
import logging
import pytest
from moto import mock_aws
import boto3
from botocore.exceptions import ClientError
import pandas as pd
from scripts.transform import (
    retrieve_s3_json,
    save_df_to_parquet_s3,
    transform_fact_players,
    transform_dim_players,
    transform_dim_teams,
    transform_dim_fixtures,
    transform_data,
)


class TestTransformFunction(unittest.TestCase):
    @patch("scripts.transform.retrieve_s3_json")
    @patch("scripts.transform.wr.s3.to_parquet")
    def test_transform_function(self, mock_df_to_parquet, mock_retrieve_json):
        current_date = datetime.now().strftime("%Y-%m-%d")

        # mock aws wrangler and extracted data lists
        mock_df_to_parquet.return_value = '{"paths": ["s3://test-bucket/2025-01-02 12:52:03/test.parquet"], "partitions_values": []}'
        mock_retrieve_json.side_effect = lambda bucket, key: {
            f"{current_date}/bootstrap-static.json": {
                "elements": [
                    {
                        "id": 1,
                        "team": 10,
                        "first_name": "John",
                        "second_name": "Doe",
                        "web_name": "JD",
                    },
                    {
                        "id": 2,
                        "team": 20,
                        "first_name": "Jane",
                        "second_name": "Smith",
                        "web_name": "JS",
                    },
                ],
                "events": [{"id": 1}, {"id": 2}],
                "teams": [
                    {"id": 10, "name": "Team A", "short_name": "TA"},
                    {"id": 20, "name": "Team B", "short_name": "TB"},
                ],
            },
            f"{current_date}/fixtures.json": [
                {
                    "id": 100,
                    "event": 1,
                    "team_h": 10,
                    "team_a": 20,
                    "team_h_difficulty": 3,
                    "team_a_difficulty": 2,
                    "kickoff_time": "2025-01-14T15:00:00Z",
                    "finished": True,
                    "team_h_score": 2,
                    "team_a_score": 1,
                }
            ],
        }[key]

        # invoke function
        transform_data("test_bucket_1", "test_bucket_2")

        # check mock was called correctly
        assert mock_df_to_parquet.call_count == 4


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


test_df = pd.DataFrame(
    [
        {
            "code": 2444613,
            "event": None,
            "finished": False,
            "finished_provisional": False,
            "id": 144,
            "kickoff_time": None,
            "minutes": 0,
            "provisional_start_time": True,
            "started": None,
            "team_a": 12,
            "team_a_score": None,
            "team_h": 8,
            "team_h_score": None,
            "stats": [],
            "team_h_difficulty": 5,
            "team_a_difficulty": 3,
            "pulse_id": 115970,
        }
    ]
)


class TestDfToParquetToS3(unittest.TestCase):
    @pytest.fixture(autouse=True)
    def inject_caplog_fixture(self, caplog):
        self._caplog = caplog

    @patch("scripts.transform.wr.s3.to_parquet")
    def test_function_uploads_file_to_s3_bucket(self, mock_df_to_parquet):
        # mock aws wrangler
        mock_df_to_parquet.return_value = '{"paths": ["s3://test-bucket/2025-01-02 12:52:03/test.parquet"], "partitions_values": []}'

        # invoke function
        save_df_to_parquet_s3("test_table", test_df, "test_bucket")

        # check mock was called correctly
        mock_df_to_parquet.assert_called_once()

    @patch("scripts.transform.wr.s3.to_parquet")
    def test_function_raises_exception_for_invalid_bucket(self, mock_to_parquet):
        self._caplog.set_level(logging.ERROR)
        # mock exception
        mock_to_parquet.side_effect = Exception("NoSuchBucket")

        # invoke function
        save_df_to_parquet_s3("test_table", test_df, "test_bucket")

        # assertion
        assert (
            "save_df_to_parquet_s3 Error processing test_table: NoSuchBucket"
            in self._caplog.text
        )


class TestTransformFactTable:
    @patch("scripts.transform.retrieve_s3_json")
    def test_returns_dataframe(self, mock_api_data):
        mock_api_data.side_effect = [
            {
                "elements": [{"team": 1, "id": 1}],
                "events": [{"id": 1}],
            },
            [
                {
                    "id": 1,
                    "event": 1,
                    "finished": True,
                    "kickoff_time": "2024-08-16T19:00:00Z",
                    "team_h": 3,
                    "team_a": 4,
                    "team_h_score": 2,
                    "team_a_score": 0,
                    "team_h_difficulty": 1,
                    "team_a_difficulty": 5,
                }
            ],
        ]

        output_df = transform_fact_players("test")

        assert isinstance(output_df, pd.DataFrame)

    @patch("scripts.transform.retrieve_s3_json")
    def test_formats_columns_correctly(self, mock_api_data):
        mock_api_data.side_effect = [
            {
                "elements": [{"team": 3, "id": 1}],
                "events": [{"id": 1}],
            },
            [
                {
                    "id": 1,
                    "event": 1,
                    "finished": True,
                    "kickoff_time": "2024-08-16T19:00:00Z",
                    "team_h": 3,
                    "team_a": 4,
                    "team_h_score": 2,
                    "team_a_score": 0,
                    "team_h_difficulty": 1,
                    "team_a_difficulty": 5,
                }
            ],
        ]

        expected_df = pd.DataFrame(
            {
                "player_id": {0: 1},
                "team_id": {0: 3},
                "gameweek_id": {0: 1},
                "fixture_id": {0: 1},
                "opposition_team_id": {0: 4},
                "fixture_difficulty_rating": {0: 1},
                "is_home": {0: True},
            }
        )

        output_df = transform_fact_players("test")
        print(output_df)
        print(expected_df)

        pd.testing.assert_frame_equal(output_df, expected_df)

    @patch("scripts.transform.retrieve_s3_json")
    def test_handles_exceptions_correctly(self, mock_api_data):
        mock_api_data.return_value = {
            "elements": [{"team": 1, "id": 1}],
            "test": [{"id": 1}],
        }
        with pytest.raises(KeyError, match="Missing required columns"):
            transform_fact_players("test")


class TestTransformDimPlayersTable:
    @patch("scripts.transform.retrieve_s3_json")
    def test_formats_columns_correctly(self, mock_api_data):
        mock_api_data.return_value = {
            "elements": [
                {
                    "first_name": "test_first",
                    "second_name": "test_second",
                    "web_name": "test",
                    "id": 1,
                    "team": 1,
                }
            ],
        }

        expected_df = pd.DataFrame(
            {
                "first_name": {0: "test_first"},
                "second_name": {0: "test_second"},
                "web_name": {0: "test"},
                "player_id": {0: 1},
                "team_id": {0: 1},
            }
        )

        output_df = transform_dim_players("test")

        pd.testing.assert_frame_equal(output_df, expected_df)

    @patch("scripts.transform.retrieve_s3_json")
    def test_handles_exceptions_correctly(self, mock_api_data):
        mock_api_data.return_value = {
            "elements": [{"team": 1, "id": 1}],
            "test": [{"id": 1}],
        }
        with pytest.raises(KeyError, match="Missing required columns"):
            transform_dim_players("test")


class TestTransformDimTeamsTable:
    @patch("scripts.transform.retrieve_s3_json")
    def test_formats_columns_correctly(self, mock_api_data):
        mock_api_data.return_value = {
            "teams": [{"id": "3", "name": "Arsenal", "short_name": "ARS"}],
        }

        expected_df = pd.DataFrame(
            {
                "team_id": {0: "3"},
                "team_name": {0: "Arsenal"},
                "team_name_short": {0: "ARS"},
            }
        )

        output_df = transform_dim_teams("test")

        pd.testing.assert_frame_equal(output_df, expected_df)

    @patch("scripts.transform.retrieve_s3_json")
    def test_handles_exceptions_correctly(self, mock_api_data):
        mock_api_data.return_value = {
            "elements": [{"team": 1, "id": 1}],
            "test": [{"id": 1}],
        }
        with pytest.raises(KeyError, match="Missing required columns"):
            transform_dim_teams("test")


class TestTransformDimFixturesTable:
    @patch("scripts.transform.retrieve_s3_json")
    def test_formats_columns_correctly(self, mock_api_data):
        mock_api_data.return_value = [
            {
                "id": 1,
                "event": 1,
                "finished": True,
                "kickoff_time": "2024-08-16T19:00:00Z",
                "team_h": 3,
                "team_a": 4,
                "team_h_score": 2,
                "team_a_score": 0,
                "team_h_difficulty": 1,
                "team_a_difficulty": 5,
            }
        ]

        expected_df = pd.DataFrame(
            {
                "fixture_id": {0: 1},
                "gameweek_id": {0: 1},
                "fixture_date": {0: pd.to_datetime("2024-08-16T19:00:00Z")},
                "fixture_time": {0: pd.to_datetime("2024-08-16T19:00:00Z")},
                "match_finished": {0: True},
                "home_team_id": {0: 3},
                "away_team_id": {0: 4},
                "home_team_score": {0: 2},
                "away_team_score": {0: 0},
                "home_team_difficulty": {0: 1},
                "away_team_difficulty": {0: 5},
            }
        )

        expected_df["fixture_date"] = expected_df["fixture_date"].dt.date
        expected_df["fixture_time"] = expected_df["fixture_time"].dt.time

        output_df = transform_dim_fixtures("test")

        pd.testing.assert_frame_equal(output_df, expected_df, check_dtype=False)

    @patch("scripts.transform.retrieve_s3_json")
    def test_handles_exceptions_correctly(self, mock_api_data):
        mock_api_data.return_value = {
            "elements": [{"team": 1, "id": 1}],
            "test": [{"id": 1}],
        }
        with pytest.raises(KeyError, match="Missing required columns"):
            transform_dim_fixtures("test")

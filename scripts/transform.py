import json
from datetime import datetime
import boto3
import awswrangler as wr
import pandas as pd
from scripts.helpers import generate_filename


def transform_data(source_bucket, destination_bucket):
    try:
        save_df_to_parquet_s3(
            "fact_players", transform_fact_players(source_bucket), destination_bucket
        )
        save_df_to_parquet_s3(
            "dim_players", transform_dim_players(source_bucket), destination_bucket
        )
        save_df_to_parquet_s3(
            "dim_teams", transform_dim_teams(source_bucket), destination_bucket
        )
        save_df_to_parquet_s3(
            "dim_fixtures", transform_dim_fixtures(source_bucket), destination_bucket
        )
    except Exception as e:
        print(e)


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
        print("retrieve_s3_json Error: ", e)
        raise


def save_df_to_parquet_s3(table_name, table_df, destination_bucket):
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
        # set the desired location for the parquet file
        path = f"s3://{destination_bucket}/{generate_filename(table_name)}.parquet"

        # convert the DataFrame to parquet and save to path in s3 bucket
        output = wr.s3.to_parquet(table_df, path)

        print("Added to bucket: ", output)
    except Exception as e:
        print(f"save_df_to_parquet_s3 Error processing {table_name}: {e}")


def transform_fact_players(bucket_name):
    try:
        current_timestamp = datetime.now().strftime("%Y-%m-%d")

        # retrieve JSON files from S3 bucket
        bootstrap_static_list = retrieve_s3_json(
            bucket_name, f"{current_timestamp}/bootstrap-static.json"
        )
        fixtures_list = retrieve_s3_json(
            bucket_name, f"{current_timestamp}/fixtures.json"
        )

        # transform lists into DataFrames
        bs_elements_df = pd.DataFrame(bootstrap_static_list["elements"])
        bs_events_df = pd.DataFrame(bootstrap_static_list["events"])
        fixtures_df = pd.DataFrame(fixtures_list)

        # create temp DataFrames with required columns
        temp_gw_df = bs_events_df[["id"]].rename(columns={"id": "gameweek_id"})
        temp_bs_df = bs_elements_df[["id", "team"]].rename(
            columns={"id": "player_id", "team": "team_id"}
        )
        # Create DataFrames for Home and Away fixtures & concatenate them
        home_fixtures = fixtures_df.rename(
            columns={
                "team_h": "team_id",
                "team_a": "opposition_team_id",
                "team_h_difficulty": "fixture_difficulty_rating",
                "event": "gameweek_id",
                "id": "fixture_id",
            }
        )
        home_fixtures["is_home"] = True

        away_fixtures = fixtures_df.rename(
            columns={
                "team_a": "team_id",
                "team_h": "opposition_team_id",
                "team_a_difficulty": "fixture_difficulty_rating",
                "event": "gameweek_id",
                "id": "fixture_id",
            }
        )
        away_fixtures["is_home"] = False

        temp_all_fixtures_df = pd.concat(
            [home_fixtures, away_fixtures], ignore_index=True
        )

        # merge temp DataFrames
        temp_players_df = pd.merge(temp_bs_df, temp_gw_df, how="cross")
        fact_players_df = pd.merge(
            temp_players_df,
            temp_all_fixtures_df,
            on=["team_id", "gameweek_id"],
            how="left",
        )

        # select specific columns
        fact_players_df = fact_players_df[
            [
                "player_id",
                "team_id",
                "gameweek_id",
                "fixture_id",
                "opposition_team_id",
                "fixture_difficulty_rating",
                "is_home",
            ]
        ]

        # drop n/a values
        fact_players_df.dropna(inplace=True)

        return fact_players_df

    except KeyError as e:
        raise KeyError(f"transform_fact_players Missing required columns: {e}")


def transform_dim_players(bucket_name):
    try:
        current_timestamp = datetime.now().strftime("%Y-%m-%d")

        # retrieve JSON files from S3 bucket
        bootstrap_static_list = retrieve_s3_json(
            bucket_name, f"{current_timestamp}/bootstrap-static.json"
        )

        # transform list into DataFrame
        dim_players_df = pd.DataFrame(bootstrap_static_list["elements"])

        # rename columns
        dim_players_df.rename(
            columns={"id": "player_id", "team": "team_id"}, inplace=True
        )

        # return DataFrame
        return dim_players_df[
            ["first_name", "second_name", "web_name", "player_id", "team_id"]
        ]

    except KeyError as e:
        raise KeyError(f"transform_dim_players Missing required columns: {e}")


def transform_dim_teams(bucket_name):
    try:
        current_timestamp = datetime.now().strftime("%Y-%m-%d")

        # retrieve JSON files from S3 bucket
        bootstrap_static_list = retrieve_s3_json(
            bucket_name, f"{current_timestamp}/bootstrap-static.json"
        )

        # transform list into DataFrame
        dim_teams_df = pd.DataFrame(bootstrap_static_list["teams"])

        # rename columns
        dim_teams_df.rename(
            columns={
                "id": "team_id",
                "name": "team_name",
                "short_name": "team_name_short",
            },
            inplace=True,
        )

        # return DataFrame
        return dim_teams_df[["team_id", "team_name", "team_name_short"]]

    except KeyError as e:
        raise KeyError(f"transform_dim_teams Missing required columns: {e}")


def transform_dim_fixtures(bucket_name):
    try:
        current_timestamp = datetime.now().strftime("%Y-%m-%d")

        # retrieve JSON files from S3 bucket
        fixtures_list = retrieve_s3_json(
            bucket_name, f"{current_timestamp}/fixtures.json"
        )

        # transform list into DataFrame
        dim_fixtures_df = pd.DataFrame(fixtures_list)

        # rename columns
        dim_fixtures_df.rename(
            columns={
                "id": "fixture_id",
                "event": "gameweek_id",
                "finished": "match_finished",
                "team_h": "home_team_id",
                "team_a": "away_team_id",
                "team_h_score": "home_team_score",
                "team_a_score": "away_team_score",
                "team_h_difficulty": "home_team_difficulty",
                "team_a_difficulty": "away_team_difficulty",
            },
            inplace=True,
        )

        # transform dates
        dim_fixtures_df["fixture_date"] = pd.to_datetime(
            dim_fixtures_df["kickoff_time"]
        ).dt.date
        dim_fixtures_df["fixture_time"] = pd.to_datetime(
            dim_fixtures_df["kickoff_time"]
        ).dt.time

        # address null values
        dim_fixtures_df["home_team_score"] = dim_fixtures_df["home_team_score"].fillna(
            0
        )
        dim_fixtures_df["away_team_score"] = dim_fixtures_df["away_team_score"].fillna(
            0
        )
        dim_fixtures_df.dropna(inplace=True)

        # return DataFrame
        return dim_fixtures_df[
            [
                "fixture_id",
                "gameweek_id",
                "fixture_date",
                "fixture_time",
                "match_finished",
                "home_team_id",
                "away_team_id",
                "home_team_score",
                "away_team_score",
                "home_team_difficulty",
                "away_team_difficulty",
            ]
        ]

    except KeyError as e:
        raise KeyError(f"transform_dim_fixtures Missing required columns: {e}")

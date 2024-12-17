import responses
import pytest
from python_files.retrieve_api_data import retrieve_data


@responses.activate
def test_successful_request():
    responses.get(
        "https://fantasy.premierleague.com/api/fixtures/",
        json={"key": "value"},
    )
    output = retrieve_data("fixtures/")
    assert output == {"key": "value"}


@responses.activate
def test_failed_request():
    responses.get(
        "https://fantasy.premierleague.com/api/test/", body=Exception("404 Not Found")
    )
    with pytest.raises(Exception) as err:
        retrieve_data("test/")
    assert str(err.value) == "404 Not Found"

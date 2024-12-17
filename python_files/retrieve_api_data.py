import requests, json


def retrieve_data(endpoint):
    try:
        base_url = 'https://fantasy.premierleague.com/api/' 
        response = requests.get(base_url+endpoint, timeout=5)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as http_err:
        print('Error:', http_err)
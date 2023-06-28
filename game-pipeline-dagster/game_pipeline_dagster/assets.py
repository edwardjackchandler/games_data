import datetime
import json
import os

import pandas as pd
import requests
from dagster import asset, get_dagster_logger

@asset(
    group_name="games",
    io_manager_key="database_io_manager",  # Addition: `io_manager_key` specified
)
def top_games():
    logger = get_dagster_logger()
    api_key = os.getenv('RAWG_API_KEY')

    page = 1
    page_size = 25
    game_limit = 100
    ordering = '-metacritic'
    games_pdf = pd.DataFrame()

    timestamp = datetime.datetime.now()
    date = datetime.date.today()

    for page in range(1, int(game_limit/page_size) + 1):
        logger.info(f'Pulling from API, {page} out of {int(game_limit/page_size)}')
        url = f'https://api.rawg.io/api/games?key={api_key}&page={page}&page_size={str(page_size)}&ordering={ordering}'
        logger.info(f'Searching URL {url}')

        cols = ['id', 'slug', 'name', 'released', 'added', 'metacritic', 'playtime']
        response = requests.get(url)
        results = response.json()['results']
        filtered_results = [ { k: v for k, v in result.items() if k in cols } for result in results]
        api_pdf = pd.read_json(json.dumps(filtered_results), orient='records')
        if games_pdf.empty:
            games_pdf = api_pdf
        else:
            games_pdf = pd.concat([games_pdf, api_pdf])

    games_pdf = games_pdf.reset_index()
    games_pdf['last_updated'] = timestamp
    return games_pdf


@asset
def top_game_ids(top_games):
    return list(top_games['id'])
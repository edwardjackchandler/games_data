import datetime
import json
import os

import pandas as pd
import requests
from dagster import asset, get_dagster_logger

@asset(
    group_name="games"
)
def top_games():
    logger = get_dagster_logger()
    api_key = os.getenv('RAWG_API_KEY')

    page = 1
    page_size = 25
    game_limit = 25
    ordering = '-released'
    games_pdf = pd.DataFrame()
    meta_critic_start = 1
    meta_critic_end = 100

    date = datetime.date.today()
    start_date = '2000-01-01'
    end_date = date.isoformat()

    timestamp = datetime.datetime.now()

    for page in range(1, int(game_limit/page_size) + 1):
        logger.info(f'Pulling from API {page} out of {int(game_limit/page_size)}')
        url = f'https://api.rawg.io/api/games?key={api_key}&page={page}&dates={start_date},{end_date}&metacritic={meta_critic_start},{meta_critic_end}&page_size={str(page_size)}&ordering={ordering}'
        logger.info(f'Searching URL {url}')

        cols = ['id', 'slug', 'name', 'released', 'added', 'metacritic', 'playtime', 'platforms', 'platform_count']
        response = requests.get(url)
        results = response.json()['results']
        logger.info(results[0])
        filtered_results = [ { k: v for k, v in result.items() if k in cols } for result in results]

        results_add_platform = []
        for item in filtered_results:
            item['platform_count'] = len(item['platforms'])
            results_add_platform.append(item)

        api_pdf = pd.read_json(json.dumps(filtered_results), orient='records')
        if games_pdf.empty:
            games_pdf = api_pdf
        else:
            games_pdf = pd.concat([games_pdf, api_pdf])

    games_pdf = games_pdf.reset_index()
    games_pdf['last_updated'] = timestamp
    del(games_pdf['index'])
    return games_pdf


@asset(
    group_name="games",
    io_manager_key="database_io_manager"
)
def top_game_database(top_games):
    return top_games


@asset(
    group_name="games",
    io_manager_key="io_manager"
)
def top_game_database(top_games):
    return top_games.to_parquet('df.parquet.gzip', compression='gzip')
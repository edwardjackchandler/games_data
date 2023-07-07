import datetime
import json
import logging

import pandas as pd
import requests

logging.basicConfig(level=logging.DEBUG)
LOG = logging.getLogger(__name__)

class RawgApi:
    """A class with a connection to the RAWG API

    Args:
        api_key str: The API Key to connect to the RAWG API
    """
    def __init__(self, api_key: str) -> None:
        self.api_key = api_key
    
    def get_top_games_dataframe(
        self, page: int, page_size: int, game_limit: int,
        start_date: str, end_date: datetime.date.today().isoformat(),
        meta_critic_start: int = 1, meta_critic_end: int = 100):

        target = 'games'

        games_pdf = pd.DataFrame()
        timestamp = datetime.datetime.now()
        query_params = locals()
        LOG.info(f"query_params: \n {query_params}")
        for page in range(1, int(game_limit/page_size) + 1):
            LOG.info(f'Pulling from API {page} out of {int(game_limit/page_size)}')
            url = self._construct_rawg_url(target, query_params)
            results = api_get(url)
            cols = ['id', 'slug', 'name', 'released', 'added', 'metacritic', 'playtime', 'platforms', 'platform_count']
            filtered_results = [ { k: v for k, v in result.items() if k in cols } for result in results]

            if not filtered_results:
                raise ValueError('filtered_results empty')
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


    def _construct_rawg_url(self, target: str, query_params: dict):
        url = f'https://api.rawg.io/api/{target}?key={self.api_key}'
        for key, value in query_params.items():
            url += f'&{key}={value}'
        return url

def api_get(url: str):
    LOG.info(f'Getting results from the following URL {url}')
    response = requests.get(url)
    results = response.json()['results']
    if not results:
        raise ValueError('Results are empty')
    LOG.info(results[0])
    return results

api_key = 'ebdbbfe35fe546308527258054eb639d'
api = RawgApi(api_key)

games_pdf = api.get_top_games_dataframe(1, 2, 2, '2022-01-01', '2022-12-31')

print(games_pdf)
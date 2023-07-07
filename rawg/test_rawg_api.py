import unittest
from unittest.mock import patch

import pandas as pd

from rawg_api import RawgApi, api_get

class TestRawgApi(unittest.TestCase):
    def setUp(self):
        self.api_key = 'test_api_key'
        self.rawg_api = RawgApi(self.api_key)

    @patch('requests.get')
    def test_get_top_games_dataframe(self, mock_get):
        mock_response = {
            'results': [
                {
                    'id': 1,
                    'slug': 'game-1',
                    'name': 'Game 1',
                    'released': '2022-01-01',
                    'added': '2022-01-01T00:00:00Z',
                    'metacritic': 90,
                    'playtime': 10,
                    'platforms': [
                        {'platform': {'id': 1, 'name': 'PC'}}
                    ]
                },
                {
                    'id': 2,
                    'slug': 'game-2',
                    'name': 'Game 2',
                    'released': '2022-02-01',
                    'added': '2022-02-01T00:00:00Z',
                    'metacritic': 80,
                    'playtime': 20,
                    'platforms': [
                        {'platform': {'id': 1, 'name': 'PC'}},
                        {'platform': {'id': 2, 'name': 'PlayStation'}}
                    ]
                }
            ]
        }
        mock_get.return_value.json.return_value = mock_response

        games_pdf = self.rawg_api.get_top_games_dataframe(
            1,
            2,
            2,
            '2022-01-01',
            '2022-12-31'
        )

        self.assertIsInstance(games_pdf, pd.DataFrame)
        self.assertEqual(len(games_pdf), 2)
        self.assertEqual(games_pdf['id'][0], 1)
        self.assertEqual(games_pdf['slug'][0], 'game-1')
        self.assertEqual(games_pdf['name'][0], 'Game 1')
        self.assertEqual(games_pdf['released'][0], '2022-01-01')
        self.assertEqual(games_pdf['added'][0], '2022-01-01T00:00:00Z')
        self.assertEqual(games_pdf['metacritic'][0], 90)
        self.assertEqual(games_pdf['playtime'][0], 10)
        self.assertEqual(games_pdf['platform_count'][0], 1)
        self.assertEqual(games_pdf['last_updated'][0].date(), datetime.date.today())


    def test_construct_rawg_url(self):
        # Test with no query parameters
        url = self.rawg_api._construct_rawg_url('games', {})
        self.assertEqual(url, 'https://api.rawg.io/api/games?key=test_api_key')

        # Test with one query parameter
        url = self.rawg_api._construct_rawg_url('games', {'page': 1})
        self.assertEqual(url, 'https://api.rawg.io/api/games?key=test_api_key&page=1')

        # Test with multiple query parameters
        url = self.rawg_api._construct_rawg_url('games', {'page': 1, 'page_size': 10})
        self.assertEqual(url, 'https://api.rawg.io/api/games?key=test_api_key&page=1&page_size=10')

    # def test_api_get(self):
    #     # Test with a valid URL
    #     url = 'https://jsonplaceholder.typicode.com/todos/1'
    #     results = api_get(url)
    #     self.assertEqual(len(results), 1)
    #     self.assertEqual(results[0]['userId'], 1)
    #     self.assertEqual(results[0]['id'], 1)
    #     self.assertEqual(results[0]['title'], 'delectus aut autem')
    #     self.assertEqual(results[0]['completed'], False)

    #     # Test with an invalid URL
    #     url = 'https://jsonplaceholder.typicode.com/invalid'
    #     with self.assertRaises(Exception):
    #         api_get(url)

if __name__ == '__main__':
    unittest.main()
import requests
import pandas as pd
import json

api_key = 'ebdbbfe35fe546308527258054eb639d'
page_size = '100'

url = f'https://api.rawg.io/api/games?key={api_key}&page_size={page_size}'
print(f'Searching URL {url}')


cols = ['id', 'slug', 'name', 'released', 'added', 'metacritic', 'playtime']
response = requests.get(url)
results = response.json()['results']
filtered_results = [ { k: v for k, v in result.items() if k in cols } for result in results]
games_pdf = pd.read_json(json.dumps(filtered_results), orient='records')
print(games_pdf)
print(games_pdf.count())

from airflow.hooks.base import BaseHook
import requests, json

def fetch_stock_prices(url, ticket='AAPL'):
    url = f'{url}{ticket}?metrics=high?&interval=1d&range=1y'
    api = BaseHook.get_connection('ENDPOINT_STOCK_API')
    response = requests.get(url, headers = api.extra_dejson['headers'])

    return json.dumps(response.json()['chart']['result'][0])
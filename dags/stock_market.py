from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.sensors.base import PokeReturnValue
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta
import requests

from include.stock_market.tasks import fetch_stock_prices

##########


@dag(
    start_date= datetime.now(),
    schedule = '@daily',
    catchup = False,
    tags = ['finance']
)
def stock_market():

    @task.sensor(poke_interval = 30, timeout = 150, mode= 'poke')
    def is_api_available() -> PokeReturnValue:
        'https://query1.finance.yahoo.com/'
        api = BaseHook.get_connection('ENDPOINT_STOCK_API')
        url = f"{api.host}{api.extra_dejson['endpoint']}"
        response = requests.get(url, headers=api.extra_dejson['headers'])

        check_health = response.json()['finance']['result'] is None
        return PokeReturnValue(is_done=check_health, xcom_value=url)
    
    get_stock_prices = PythonOperator(
        task_id = 'get_stock_price',
        python_callable = fetch_stock_prices,
        op_kwargs = {'url':'{{task_instance.xcom_pull(task_ids="is_api_available")}}', 'ticket':'AAPL'}
    )

    is_api_available() >> get_stock_prices

stock_market()
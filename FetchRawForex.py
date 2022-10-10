from airflow.decorators import dag, task
import pendulum
import requests
import json
import datetime
from airflow.providers.postgres.hooks.postgres import PostgresHook

TABLE_PG = "forex_raw_eurusd"
URL = "https://twelve-data1.p.rapidapi.com/time_series"
PARAMS = {"symbol": "EUR/USD",
          "interval": "1min",
          "outputsize": "30",
          "format": "json"}
HEADERS = {
    "X-RapidAPI-Key": "f452e61143msh123aa1934dc90a6p12848bjsn8b09796dd8a2",
    "X-RapidAPI-Host": "twelve-data1.p.rapidapi.com"
}


@dag(
    dag_id='fetch_forex_eurusd',
    schedule_interval='0/30 * * * *',
    start_date=pendulum.datetime(2022, 10, 7),
    catchup=False,
    tags=["fetch_raw_EURUSD", "Currency"]
)
def fetch_forex_futures():

    @task()
    def insert_response(table: str, data: str):
        hook = PostgresHook(postgres_conn_id='pg_conn_airflow')
        sql = """
                INSERT INTO %s (response_time, response_body)
                values ('%s', '%s')
                """
        hook.run(sql % (table, str(datetime.datetime.now()), data))

    @task()
    def get_response():
        response = requests.request("GET", URL, headers=HEADERS, params=PARAMS)
        data = json.loads(response.text)
        return str(data).replace("'", '"')

    resp = get_response()
    insert_response(TABLE_PG, resp)


run = fetch_forex_futures()

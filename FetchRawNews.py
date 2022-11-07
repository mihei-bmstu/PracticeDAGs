from airflow.decorators import dag, task
import pendulum
import requests
import json
import datetime
from airflow.providers.postgres.hooks.postgres import PostgresHook

TABLE_PG = "news_raw"
URL = "https://latest-news3.p.rapidapi.com/Latest"
PARAMS = {"language": "en"}
HEADERS = {
    "X-RapidAPI-Key": "f452e61143msh123aa1934dc90a6p12848bjsn8b09796dd8a2",
    "X-RapidAPI-Host": "latest-news3.p.rapidapi.com"
}


@dag(
    dag_id='fetch_raw_news',
    schedule_interval='0/30 * * * *',
    start_date=pendulum.datetime(2022, 10, 31),
    catchup=False,
    tags=["raw_news", "requests"]
)
def fetch_raw_news():
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
        response = requests.request("GET", URL, headers=HEADERS)
        data = json.loads(response.text)
        return str(data).replace('"', "").replace("'", '"').replace("\\n", "")

    resp = get_response()
    insert_response(TABLE_PG, resp)


run = fetch_raw_news()

from airflow.decorators import dag, task
import pendulum
import requests
import json
from airflow.providers.postgres.hooks.postgres import PostgresHook

CITY = "moscow"
COLUMNS = ["dateTimeISO", "tempC", "humidity", "pressureMB", "windSpeedKPH", "weather"]
URL_OB = "https://aerisweather1.p.rapidapi.com/observations/" + CITY + ",%20rus"
URL_FOR = "https://aerisweather1.p.rapidapi.com/forecasts/" + CITY + ",%20rus"
QUERY_STRING = {"plimit": "1", "filter": "1hr"}
HEADERS = {
    "X-RapidAPI-Key": "f452e61143msh123aa1934dc90a6p12848bjsn8b09796dd8a2",
    "X-RapidAPI-Host": "aerisweather1.p.rapidapi.com"
}


@dag(
    dag_id='load_weather_observation',
    schedule_interval='30 * * * *',
    start_date=pendulum.datetime(2022, 1, 1),
    catchup=False,
)
def load_weather():
    @task()
    def get_current_weather():
        response = requests.request("GET", URL_OB, headers=HEADERS)
        data = json.loads(response.text)
        current_row = []
        for col in COLUMNS:
            current_row.append(data["response"]["ob"][col])
        return current_row

    @task()
    def insert_weather(row: list, table: str):
        hook = PostgresHook(postgres_conn_id='pg_conn_weather')
        sql = """
                INSERT INTO %s (dateTimeISO, tempC, humidity, pressureMB, windSpeedKPH, weather)
                values ('%s', %d, %d, %d, %d, '%s')
                """
        hook.run(sql % (table, *row))

    crow = get_current_weather()
    insert_weather(crow, CITY + '_ob')


weather = load_weather()

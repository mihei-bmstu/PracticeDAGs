from airflow.decorators import dag, task
import pendulum
import requests
import json
import datetime
from airflow.providers.postgres.hooks.postgres import PostgresHook

TABLES_PG = ("quote_brent", "quote_lumber", "quote_gold", "quote_platinum", "quote_silver")
COMMODITIES = ("Brent Oil", "Lumber", "Gold", "Platinum", "Silver")
TRANSLATE_TABLES = {"Brent Oil": "quote_brent",
                    "Lumber": "quote_lumber",
                    "Gold": "quote_gold",
                    "Platinum": "quote_platinum",
                    "Silver": "quote_silver"}
COLUMNS_PG = ["date_time", "last", "high", "low", "change_abs", "change_per", "expiration"]
COLUMNS_API = ["Time", "Last", "High", "low", "Chg.", "Chg. %", "Month"]
TRANSLATE_COLUMNS = {"Time": "date_time",
                     "Last": "last",
                     "High": "high",
                     "Low": "low",
                     "Chg.": "change_abs",
                     "Chg. %": "change_per",
                     "Month": "expiration"}
URL = "https://investing4.p.rapidapi.com/commodities/commodity-futures-prices"
HEADERS = {
    "X-RapidAPI-Key": "f452e61143msh123aa1934dc90a6p12848bjsn8b09796dd8a2",
    "X-RapidAPI-Host": "investing4.p.rapidapi.com"
}


@dag(
    dag_id='fetch_commodities_futures',
    schedule_interval='0/15 * * * *',
    start_date=pendulum.datetime(2022, 1, 1),
    catchup=False,
)
def fetch_commodities_futures():
    def get_empty_dict():
        return {"date_time": "",
                "last": 0.0,
                "high": 0.0,
                "low": 0.0,
                "change_abs": 0.0,
                "change_per": 0.0,
                "expiration": ""}

    def fetch_row(com_entity: dict):
        current_row = get_empty_dict()
        for (k, v) in com_entity.items():
            if k in TRANSLATE_COLUMNS.keys():
                current_row[TRANSLATE_COLUMNS[k]] = v
        current_row["date_time"] = str(datetime.datetime.now())
        return current_row

    @task()
    def insert_quotes(quotes: dict):
        hook = PostgresHook(postgres_conn_id='pg_conn_airflow')
        for (table, row) in quotes.items():
            sql = """
                    INSERT INTO %s (date_time, last, high, low, change_abs, change_per, expiration)
                    values ('%s', %f, %f, %f, %f, '%s', '%s')
                    """
            hook.run(sql % (table, *row.values()))

    @task()
    def get_response():
        response = requests.request("GET", URL, headers=HEADERS)
        data = json.loads(response.text)
        current = dict.fromkeys(TABLES_PG)
        for com in data["data"]:
            if com["Commodity"] in TRANSLATE_TABLES.keys():
                current[TRANSLATE_TABLES[com["Commodity"]]] = fetch_row(com)
        return current

    quotes = get_response()
    insert_quotes(quotes)


weather = fetch_commodities_futures()

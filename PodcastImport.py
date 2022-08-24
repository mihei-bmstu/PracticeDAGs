import os
from airflow.decorators import dag, task
import pendulum
import requests
import xmltodict
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


@dag(
    dag_id='podcast_summary',
    schedule_interval='@daily',
    start_date=pendulum.datetime(2022, 1, 1),
    catchup=False,
)
def podcast_summary():

    create_database = PostgresOperator(
        task_id="create_table_postgres",
        sql=r"""
        CREATE TABLE IF NOT EXISTS episodes (
            link VARCHAR PRIMARY KEY,
            title VARCHAR,
            filename VARCHAR,
            published VARCHAR,
            description VARCHAR
            )
        """,
        postgres_conn_id='tutorial_pg_conn'
    )

    @task()
    def get_episodes():
        data = requests.get("https://www.marketplace.org/feed/podcast/marketplace/")
        feed = xmltodict.parse(data.text)
        episodes = feed["rss"]["channel"]["item"]
        print(f"Found {len(episodes)} episodes.")
        return episodes

    @task()
    def load_episodes(episodes):
        hook = PostgresHook(postgres_conn_id='tutorial_pg_conn')
        stored = hook.get_pandas_df("SELECT * FROM episodes;")
        new_episodes = []
        for e in episodes:
            if e["link"] not in stored["link"].values:
                filename = f"{e['link'].split('/')[-1]}.mp3"
                new_episodes.append([
                    e["link"],
                    e["title"],
                    e["pubDate"],
                    e["description"],
                    filename
                ])
        hook.insert_rows(table="episodes",
                         rows=new_episodes,
                         target_fields=["link", "title", "published", "description", "filename"]
                         )

    # @task()
    # def download_episodes(episodes):
    #     for e in episodes:
    #         filename = f"{e['link'].split('/')[-1]}.mp3"
    #         audio_path = os.path.join("episodes", filename)
    #         if not os.path.exists(audio_path):
    #             print(f"Downloading {filename}")
    #             audio = requests.get(e["enclosure"]["@url"])
    #             with open(audio_path, "wb+") as f:
    #                 f.write(audio.content)

    podcast_episodes = get_episodes()
    create_database.set_downstream(podcast_episodes)
    load_episodes(podcast_episodes)
    #download_episodes(podcast_episodes)


summary = podcast_summary()

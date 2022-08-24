from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.dummy_operator import DummyOperator
import pendulum


local_tz = pendulum.timezone("Europe/Moscow")

default_args = {
    'owner': 'airflow',
    'provide_context': True,
    'depends_on_past': False,
    'start_date': datetime(2022, 8, 12, 1, 0, tzinfo=local_tz)
}

dag = DAG(
    'LoadWeather',
    default_args=default_args,
    description='Load hotel-weather table from local file to PG',
    schedule_interval=None,
    tags=['LoadWeather']
)

dag.doc_md = """
Load hotel-weather table from local file to PG
"""

start_DAG = DummyOperator(
    task_id='start',
    dag=dag)

submit_spark_job = SparkSubmitOperator(
    application="/usr/local/spark/resources/AirflowSubmit-assembly-0.1.0.jar",
    name="calc",
    conf={'spark.submit.deployMode': 'cluster',
          'spark.driver.memory': '1g',
          'spark.executor.memory': '1g',
          'spark.executor.cores': '1',
          'spark.executor.instances': '2',
          'master': 'spark://spark:7077'
          },
    task_id="submit_calc",
    conn_id="spark_default",
    java_class="Boot",
    dag=dag
)
start_DAG >> submit_spark_job

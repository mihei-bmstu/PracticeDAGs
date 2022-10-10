from datetime import datetime
from airflow.models import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.filesystem import FileSensor
import pendulum


local_tz = pendulum.timezone("Europe/Moscow")

default_args = {
    'owner': 'airflow',
    'provide_context': True,
    'depends_on_past': False,
    'start_date': datetime(2022, 10, 10, 1, 0, tzinfo=local_tz),
    'catchup': False
}

dag = DAG(
    'ETLForexEurUsd',
    default_args=default_args,
    description='ETL raw forex eurusd',
    schedule_interval='30 * * * *',
    tags=['ETL', 'EurUsd']
)

dag.doc_md = """
ETL raw forex eurusd
"""

start_DAG = EmptyOperator(
    task_id='start',
    dag=dag)

submit_spark_job = SparkSubmitOperator(
    application="/usr/local/spark/resources/MeanQuotes-assembly-0.1.0.jar",
    name="SparkMACalculation",
    conf={'spark.submit.deployMode': 'cluster',
          'spark.driver.memory': '1g',
          'spark.executor.memory': '1g',
          'spark.executor.cores': '1',
          'spark.executor.instances': '1',
          'master': 'spark://spark:7077'
          },
    task_id="submit_calc",
    conn_id="spark_default",
    java_class="Boot",
    dag=dag
)


start_DAG >> submit_spark_job

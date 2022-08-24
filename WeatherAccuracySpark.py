from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.empty import EmptyOperator
import pendulum


local_tz = pendulum.timezone("Europe/Moscow")

default_args = {
    'owner': 'airflow',
    'provide_context': True,
    'depends_on_past': False,
    'start_date': datetime(2022, 8, 12, 1, 0, tzinfo=local_tz),
    'catchup': False
}

dag = DAG(
    'WeatherAccuracy',
    default_args=default_args,
    description='Calculate weather forecast accuracy',
    schedule_interval='0 17 * * *',
    tags=['SparkWeatherAccuracy']
)

dag.doc_md = """
Calculate weather forecast accuracy
"""

start_DAG = EmptyOperator(
    task_id='start',
    dag=dag)

submit_spark_job = SparkSubmitOperator(
    application="/usr/local/spark/resources/WeatherAccuracy-assembly-0.1.0.jar",
    name="SparkCalculation",
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

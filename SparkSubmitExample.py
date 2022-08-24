from datetime import datetime
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
    'ExamplePi',
    default_args=default_args,
    description='Calc Pi',
    schedule_interval=None,
    tags=['Pi']
)

dag.doc_md = """
Test run of Pi calculations
"""

start_DAG = DummyOperator(
    task_id='start',
    dag=dag)

submit_spark_job = SparkSubmitOperator(
    application="/usr/local/spark/resources/spark-examples_2.12-3.1.1-hadoop-2.7.jar",
    name="calc",
    conf={'spark.submit.deployMode': 'cluster',
          'spark.driver.memory': '512m',
          'spark.executor.memory': '512m',
          'spark.executor.cores': '1',
          'spark.dynamicAllocation.enabled': 'true',
          'spark.shuffle.service.enabled': 'true',
          'spark.dynamicAllocation.maxExecutors': '1',
          'master': 'spark://spark:7077'
          },
    task_id="submit_Pi_calc",
    conn_id="spark_default",
    java_class="org.apache.spark.examples.SparkPi",
    dag=dag
)
start_DAG >> submit_spark_job

import logging
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'ds',
    'poke_interval': 600
}

with DAG("spark_test",
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['ds']
) as dag:

    dummy = DummyOperator(task_id="dummy")

    spark_submit_local = SparkSubmitOperator(
        application='/opt/spark-apps/spark.jar',
        java_class='ru.filit.connection_test.Main',
        conn_id='spark_default',
        task_id='spark_submit_task'
    )


dummy >> spark_submit_local
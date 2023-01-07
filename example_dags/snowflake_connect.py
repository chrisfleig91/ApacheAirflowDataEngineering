import logging
from datetime import datetime, timedelta
import airflow
from airflow import DAG
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.python import PythonOperator

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

args = {"owner": "Airflow", "start_date": datetime(2021,3,22,17,15)}

dag = DAG(
    dag_id="snowflake_connect", default_args=args, schedule='@hourly'
)

query1 = [
    """select 1;""",
    """show tables in database snowflake_sample_data;""",
]


def count1(**context):
    dwh_hook = SnowflakeHook(snowflake_conn_id="Snowflake Sample Data")
    result = dwh_hook.get_first("select count(*) from snowflake_sample_data.weather.daily_14_total;")
    logging.info("Number of rows in `snowflake_sample_data.weather.daily_14_total`  - %s", result[0])


with dag:
    query1_exec = SnowflakeOperator(
        task_id="snowfalke_task1",
        sql=query1,
        snowflake_conn_id="Snowflake Sample Data",
    )

    count_query = PythonOperator(task_id="count_query", python_callable=count1)
query1_exec >> count_query
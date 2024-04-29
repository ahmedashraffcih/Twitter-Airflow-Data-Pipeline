from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
from twitter_etl import run_twitter_etl

default_args = {
    'owner': 'ahmedashraffcih',
    'depends_on_past': False,
    'start_date': datetime(2020, 11, 8),
    'email': ['ahmedashraffcih@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'twitter_dag',
    default_args=default_args,
    description='My First Twitter API',
    schedule_interval='@weekly'
)

def run_etl():
    run_twitter_etl(screen_name='@elonmusk', output_path='s3://twitter-airflow-bucket/elonmusk_twitter_data.csv')

run_etl_task = PythonOperator(
    task_id='complete_twitter_etl',
    python_callable=run_etl,
    dag=dag,
)

run_etl_task

from datetime import datetime, timedelta

from sqlalchemy import create_engine
from csbiETL.common.DataLake import DataLake
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from io import BytesIO
from csbiETL import config
from csbiETL.etl.landing.frontline.ld_frontline import ld_email_frontline
from pathlib import Path
import sys



mysql_context = create_engine(config.MYSQL_CONNECTION_STRING)


schedule_interval = '*/5 * * * *'
dag_name = 'datalaketest'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 11, 6, 10, 10),
    'execution_timeout'=timedelta(minutes=1)
}

def send():
    folder = Path(__file__).resolve().parents[0]
    p = f'{folder}/Frontline_Daily_2019-11-05.xlsx'
    b= BytesIO(b'asd')

    with open(p, 'rb') as f:
        DataLake(debug_mode=False).upload_file(b, 'fas', '12')

dag = DAG(dag_name, default_args=default_args, schedule_interval=schedule_interval)

ld_frontline = PythonOperator(
    task_id='ld_email_frontline',
    provide_context=False,
    python_callable=send,
    dag=dag
)

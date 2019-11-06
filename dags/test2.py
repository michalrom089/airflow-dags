from datetime import datetime


from csbiETL.common.DataLake import DataLake
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from io import BytesIO


schedule_interval = '*/5 * * * *'
dag_name = 'datalaketest'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 11, 6, 10, 10)
}

def send():
    b = BytesIO(b'asdasd')
    DataLake(debug_mode=False).upload_file(b, 'test2', 't.2')

dag = DAG(dag_name, default_args=default_args, schedule_interval=schedule_interval)

wh_pbi = PythonOperator(
    task_id='send1',
    provide_context=False,
    python_callable=send,
    dag=dag
)




from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from sqlalchemy import create_engine

from csbiETL.etl.warehouse.pbi_rls.wh_pbi_rls import wh_powerbi_rls
from csbiETL import config


schedule_interval = '0 5 * * *'
dag_name = 'wh_pbi_rls'
cdw_context = create_engine(config.CDW_CONNECTION_STRING)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 11, 5)
}

dag = DAG(dag_name, default_args=default_args, schedule_interval=schedule_interval)

wh_pbi = PythonOperator(
    task_id='wh_powerbi_rls',
    provide_context=False,
    python_callable=wh_powerbi_rls,
    op_kwargs={'db_context': cdw_context},
    dag=dag
)

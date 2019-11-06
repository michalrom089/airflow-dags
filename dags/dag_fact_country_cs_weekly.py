from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from sqlalchemy import create_engine

from csbiETL.etl.landing.cdw.ld_weekly import ld_cdw_weekly
from csbiETL.etl.staging.cdw.stg_cdw_weekly import stg_cdw_weekly
from csbiETL.etl.warehouse.fact_country.wh_fact_country_cs_weekly import wh_fact_country_cs_weekly
from csbiETL import config


schedule_interval = '0 5 * * Mon'
dag_name = 'wh_fact_country_cs_weekly'
cdw_context = create_engine(config.CDW_CONNECTION_STRING)
mysql_context = create_engine(config.MYSQL_CONNECTION_STRING)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 11, 5)
}

dag = DAG(dag_name, default_args=default_args, schedule_interval=schedule_interval)

ld_weekly = PythonOperator(
    task_id='ld_cdw_weekly',
    provide_context=False,
    python_callable=ld_cdw_weekly,
    op_kwargs={'db_context': mysql_context},
    dag=dag
)

stg_weekly = PythonOperator(
    task_id='stg_cdw_weekly',
    provide_context=False,
    python_callable=stg_cdw_weekly,
    op_kwargs={'db_context': mysql_context},
    dag=dag
)

wh_weekly = PythonOperator(
    task_id='wh_fact_country_cs_weekly',
    provide_context=False,
    python_callable=wh_fact_country_cs_weekly,
    op_kwargs={'db_src': mysql_context, 'db_dest': cdw_context},
    dag=dag
)

ld_weekly >> stg_weekly >> wh_weekly

from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from sqlalchemy import create_engine

from csbiETL.etl.landing.incontact.ld_telesales import ld_telesales
from csbiETL.etl.landing.sap.ld_sap_telesales import ld_sap_telesales
from csbiETL.etl.staging.incontact.stg_telesales import stg_telesales
from csbiETL.etl.staging.sap.stg_sap_telesales import stg_sap_telesales
from csbiETL.etl.warehouse.fact_country.wh_fact_country_telesales import wh_fact_country_telesales
from csbiETL import config


schedule_interval = '0 5 * * *'
dag_name = 'wh_fact_country_cs_telesales'
cdw_context = create_engine(config.CDW_CONNECTION_STRING)
mysql_context = create_engine(config.MYSQL_CONNECTION_STRING)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 11, 5)
}

dag = DAG(dag_name, default_args=default_args, schedule_interval=schedule_interval)
    
ld_sap = PythonOperator(
    task_id='ld_sap_telesales',
    provide_context=False,
    python_callable=ld_sap_telesales,
    op_kwargs={'db_context': mysql_context},
    dag=dag
)

ld_incontact = PythonOperator(
    task_id='ld_telesales',
    provide_context=False,
    python_callable=ld_telesales,
    op_kwargs={'db_context': mysql_context},
    dag=dag
)

stg_sap = PythonOperator(
    task_id='stg_sap_telesales',
    provide_context=False,
    python_callable=stg_sap_telesales,
    op_kwargs={'db_context': mysql_context},
    dag=dag
)

stg_incontact = PythonOperator(
    task_id='stg_telesales',
    provide_context=False,
    python_callable=stg_telesales,
    op_kwargs={'db_context': mysql_context},
    dag=dag
)

wh_telesales = PythonOperator(
    task_id='wh_fact_country_telesales',
    provide_context=False,
    python_callable=wh_fact_country_telesales,
    op_kwargs={'db_src': mysql_context, 'db_dest': cdw_context},
    dag=dag
)

ld_sap >> stg_sap >> wh_telesales
ld_incontact >> stg_incontact >> wh_telesales

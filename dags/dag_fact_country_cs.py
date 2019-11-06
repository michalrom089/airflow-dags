from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from sqlalchemy import create_engine

from csbiETL import config
from csbiETL.etl.landing.kana.ld_all import ld_kana_all
from csbiETL.etl.landing.fr_onl_sales.ld_fr_onl_sales import ld_email_fr_onl_sales
from csbiETL.etl.landing.sap.ld_sap_nordics import ld_sap_nordics
from csbiETL.etl.landing.iex.ld_forecast import ld_iex_forecast
from csbiETL.etl.landing.iex.ld_results import ld_iex_results
from csbiETL.etl.landing.iex.ld_schedule import ld_iex_schedule
from csbiETL.etl.landing.incontact.ld_cs_performance import ld_email_cs_performance
from csbiETL.etl.landing.incontact.ld_cs_productivity import ld_email_cs_productivity
from csbiETL.etl.landing.incontact.ld_cs_unavailable import ld_email_cs_unavailable
from csbiETL.etl.landing.powerfront.ld_channel import ld_powerfront_channel

from csbiETL.etl.staging.kana.stg_all import stg_kana_all
from csbiETL.etl.staging.extra_sales.stg_extra_sales import stg_extra_sales
from csbiETL.etl.staging.iex.stg_forecast import stg_iex_forecast
from csbiETL.etl.staging.iex.stg_results import stg_iex_results
from csbiETL.etl.staging.iex.stg_schedule import stg_iex_schedule
from csbiETL.etl.staging.incontact.stg_cs_performance import stg_cs_performance
from csbiETL.etl.staging.incontact.stg_cs_productivity import stg_cs_productivity
from csbiETL.etl.staging.incontact.stg_cs_unavailable import stg_cs_unavailable
from csbiETL.etl.staging.powerfront.stg_all import stg_powerfront_all

from csbiETL.etl.warehouse.fact_country.wh_fact_country_cs import wh_fact_country_cs

schedule_interval = '0 5 * * *'
dag_name = 'fact_country_cs'
cdw_context = create_engine(config.CDW_CONNECTION_STRING)
mysql_context = create_engine(config.MYSQL_CONNECTION_STRING)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 11, 5)
}


ld_func = [
    ld_kana_all, #kana
    ld_email_cs_performance, #email_cs
    ld_email_cs_productivity,
    ld_email_cs_unavailable,
    ld_iex_forecast, #iex
    ld_iex_results,
    ld_iex_schedule,
    ld_email_fr_onl_sales, #extra_sales
    ld_sap_nordics,
    ld_powerfront_channel
]

stg_func = [
    stg_kana_all,
    stg_extra_sales,
    stg_iex_forecast,
    stg_iex_results,
    stg_iex_schedule,
    stg_cs_performance,
    stg_cs_productivity,
    stg_cs_unavailable,
    stg_powerfront_all
]

wh_func = wh_fact_country_cs

dag = DAG(dag_name, default_args=default_args, schedule_interval=schedule_interval)
    
ld_done = DummyOperator(task_id='landing_done', dag=dag)
stg_done = DummyOperator(task_id='stg_done', dag=dag)

for m in ld_func:
    t = PythonOperator(
        task_id=m.__name__,
        provide_context=False,
        python_callable=m,
        op_kwargs={'db_context': mysql_context},
        dag=dag
    )

    t >> ld_done

for m in stg_func:
    t = PythonOperator(
        task_id=m.__name__,
        provide_context=False,
        python_callable=m,
        op_kwargs={'db_context': mysql_context},
        dag=dag
    )

    ld_done >> t >> stg_done

wh = PythonOperator(
    task_id=wh_func.__name__,
    provide_context=False,
    python_callable=wh_func,
    op_kwargs={'db_src': mysql_context, 'db_dest': cdw_context},
    dag=dag
)

stg_done >> wh

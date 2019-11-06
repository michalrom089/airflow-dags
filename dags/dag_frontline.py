from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from sqlalchemy import create_engine

from csbiETL.etl.landing.frontline.ld_frontline import ld_email_frontline
from csbiETL.etl.staging.frontline.stg_frontline import stg_emails_frontline
from csbiETL.etl.warehouse.frontline.wh_frontline import wh_emails_frontline
from csbiETL import config


schedule_interval = '0 5 * * *'
dag_name = 'wh_frontline'
cdw_context = create_engine(config.CDW_CONNECTION_STRING)
mysql_context = create_engine(config.MYSQL_CONNECTION_STRING)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 11, 5)
}

dag = DAG(dag_name, default_args=default_args, schedule_interval=schedule_interval)

# ld_frontline = PythonOperator(
#     task_id='ld_email_frontline',
#     provide_context=False,
#     python_callable=ld_email_frontline,
#     op_kwargs={'db_context': mysql_context},
#     dag=dag
# )

# stg_frontline = PythonOperator(
#     task_id='stg_emails_frontline',
#     provide_context=False,
#     python_callable=stg_emails_frontline,
#     op_kwargs={'db_context': mysql_context},
#     dag=dag
# )

wh_frontline = PythonOperator(
    task_id='wh_emails_frontline',
    provide_context=False,
    python_callable=wh_emails_frontline,
    op_kwargs={'db_src': mysql_context, 'db_dest': cdw_context},
    dag=dag
)

# ld_frontline >> stg_frontline >> wh_frontline

import airflow
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from csbiETL import config

YESTERDAY = datetime.now() - timedelta(days=1)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(2),
}

dag = DAG(
    'python_test', default_args=default_args, schedule_interval=None)


def print_context():
    print("mysql_conn_string:")
    print(config.MYSQL_CONNECTION_STRING)
    return 'Whatever you return gets printed in the logs'


run_this = PythonOperator(
    task_id='print_the_context',
    provide_context=False,
    python_callable=print_context,
    dag=dag,
)

if __name__ == "__main__":
    dag.cli()

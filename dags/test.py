
from datetime import datetime, timedelta

import airflow
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator

args = {
    'owner': 'Airflow',
    'start_date': datetime(2019,11,6,9,0)
}

dag = DAG(
    dag_id='example_bash_operator',
    default_args=args,
    schedule_interval='*/5 * * * *',
    dagrun_timeout=timedelta(minutes=60),
)

run_this_last = DummyOperator(
    task_id='run_this_last',
    dag=dag,
)

# [START howto_operator_bash]
run_this = BashOperator(
    task_id='run_after_loop',
    bash_command='echo 1',
    dag=dag,
)

run_this >> run_this_last

if __name__ == "__main__":
    dag.cli()
from airflow import DAG
from datetime import datetime, timedelta
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator

YESTERDAY = datetime.now() - datetime.timedelta(days=1)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': YESTERDAY,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'kube2', default_args=default_args, schedule_interval=timedelta(minutes=10))


last = DummyOperator(task_id='run_last', dag=dag)

passing = KubernetesPodOperator(namespace='airflow',
                                image="python:3.7",
                                cmds=["Python", "-c"],
                                arguments=["print('hello world')"],
                                labels={"foo": "bar"},
                                name="passing-test",
                                task_id="passing-task",
                                get_logs=True,
                                dag=dag
                                )

failing = KubernetesPodOperator(namespace='airflow',
                                image="ubuntu:1604",
                                cmds=["Python", "-c"],
                                arguments=["print('hello world')"],
                                labels={"foo": "bar"},
                                name="fail",
                                task_id="failing-task",
                                get_logs=True,
                                dag=dag
                                )

task1 = BashOperator(
    task_id='run_first',
    bash_command='echo 1',
    dag=dag,
)

task1 >> passing
passing >> last
failing >> last


if __name__ == "__main__":
    dag.cli()

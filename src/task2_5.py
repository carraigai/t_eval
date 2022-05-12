from airflow.operators.dummy import DummyOperator
from airflow.models import DAG
from datetime import datetime

dag = DAG('my_dag', start_date=datetime(2022, 5, 12))

task_1 = DummyOperator(
    task_id='task_1',
    dag=dag,
)

task_2 = DummyOperator(
    task_id='task_2',
    dag=dag,
)

task_3 = DummyOperator(
    task_id='task_3',
    dag=dag,
)

task_4 = DummyOperator(
    task_id='task_4',
    dag=dag,
)

task_5 = DummyOperator(
    task_id='task_5',
    dag=dag,
)

task_6 = DummyOperator(
    task_id='task_6',
    dag=dag,
)

task_1 >> [task_2, task_3]
task_2 >> [task_4, task_5, task_6]
task_3 >> [task_4, task_5, task_6]

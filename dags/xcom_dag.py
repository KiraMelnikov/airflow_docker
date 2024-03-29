from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.subdag import SubDagOperator
from airflow.utils.task_group import TaskGroup

from random import uniform
from datetime import datetime

default_args = {
    'start_date': datetime(2020, 1, 1)
}

def _training_model(ti):
    accuracy = uniform(0.1, 10.0)
    ti.xcom_push(key='accuracy', value=accuracy)
    print(f'model\'s accuracy: {accuracy}')
    return accuracy # optional

def _choose_best_model(ti):
    key='accuracy'
    tasks = ['processing_tasks.training_model_a','processing_tasks.training_model_b','processing_tasks.training_model_c']
    result = []
    for task in tasks:
        print(f"Task: {task}")
        pull=ti.xcom_pull(key=key, task_ids=task)
        print(f"Getting the pull: {pull}")
        result.append(round(pull,3))
    print(f"Result var: {result[-1]}")
    # max_accuracy = result.sort()[-1]
    # print(f'Choose best model\n{max_accuracy}')

with DAG('xcom_dag', schedule_interval='@daily', default_args=default_args, catchup=False) as dag:
    downloading_data = BashOperator(
        task_id='downloading_data',
        bash_command='sleep 3'
    )

    with TaskGroup('processing_tasks') as processing_tasks:
        training_model_a = PythonOperator(
            task_id='training_model_a',
            python_callable=_training_model
        )

        training_model_b = PythonOperator(
            task_id='training_model_b',
            python_callable=_training_model
        )

        training_model_c = PythonOperator(
            task_id='training_model_c',
            python_callable=_training_model
        )

    choose_model = PythonOperator(
        task_id='task_4',
        python_callable=_choose_best_model
    )

    downloading_data >> processing_tasks >> choose_model
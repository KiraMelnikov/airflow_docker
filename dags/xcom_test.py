from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
 
from datetime import datetime
 
def _t1(ti):
    response = 2+2
    ti.xcom_push(key='test_key', value=response)
 
def _t2(ti):
    response = ti.xcom_pull(key='test_key', task_ids='t1')
    print(response)

def _branch(ti):
    value = ti.xcom_pull(key='test_key', task_ids='t1')
    if value == 4:
        return 't2'
    else:
        return 't3'
 
with DAG("xcom_dag_test", start_date=datetime(2022, 1, 1), 
    schedule_interval='@daily', catchup=False) as dag:
 
    t1 = PythonOperator(
        task_id='t1',
        python_callable=_t1
    )

    branch = BranchPythonOperator(
        task_id='branch',
        python_callable=_branch
    )
 
    t2 = PythonOperator(
        task_id='t2',
        python_callable=_t2
    )
 
    t3 = BashOperator(
        task_id='t3',
        bash_command="echo ''"
    )
 
    t4 = BashOperator(
        task_id='t4',
        bash_command="echo ''",
        trigger_rule='all_success' # it has 10 rules but by default is 'all_success' 
    )
 
    t5 = BashOperator(
        task_id='t5',
        bash_command="echo ''"
    )
 
    t1 >> branch >> [t2, t3] #[[t2,t4] [t3, t5]]
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.subdag import SubDagOperator
from datetime import datetime
from subdags.subdag_downloads import subdag_downloads
from grouptasks.group_transforms import transform_tasks

with DAG('group_dag', start_date=datetime(2022, 1, 1), 
    schedule_interval='@daily', catchup=False) as dag:
 
    args = {'start_date':dag.start_date, 'schedule_interval':dag.schedule_interval, 'catchup':dag.catchup}

    # first consept by SubDagOperator()
    downloads = SubDagOperator(
        task_id='downloads',
        subdag=subdag_downloads(dag.dag_id, 'downloads', args)
    )

    check_files = BashOperator(
        task_id='check_files',
        bash_command='sleep 10'
    )
    
    # second consept by TaskGroups()
    transforms = transform_tasks(group_id='transforms')
 
 
    downloads >> check_files >> transforms
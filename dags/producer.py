from airflow import DAG, Dataset
from airflow.decorators import task
from include.datasets import MY_FILE
from datetime import datetime


with DAG(
    dag_id = "producer_test",
    schedule='@daily',
    start_date=datetime(2024,1,1),
    catchup=False
) as dag:

    @task(outlets=[MY_FILE])
    def update_dataset():
        with open(MY_FILE.uri, "a+") as f:
            f.write("producer update")
    
    update_dataset()
from airflow import DAG, Dataset
from airflow.decorators import task
from datetime import datetime
from include.datasets import MY_FILE

with DAG(
    dag_id = "consumer_test",
    schedule=[MY_FILE],
    start_date=datetime(2024,1,1),
    catchup=False,
    tags=['dataset']
) as dag:

    @task
    def read_dataset():
        with open(MY_FILE.uri, "r") as f:
            print(f.read())
    
    read_dataset()
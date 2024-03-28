from airflow import DAG, Dataset
from airflow.decorators import task

from datetime import datetime


my_file_1 = Dataset("s3://dag1/output_1.txt")

with DAG(
    dag_id = "consumer_test",
    schedule=[my_file_1],
    start_date=datetime(2024,1,1),
    catchup=False
) as dag:

    @task
    def read_dataset():
        with open(my_file_1.uri, "r") as f:
            print(f.read())
    
    read_dataset()
from airflow import DAG, Dataset
from airflow.decorators import task

from datetime import datetime


my_file_1 = Dataset("s3://dag1/output_1.txt")

with DAG(
    dag_id = "producer_test",
    schedule='@daily',
    start_date=datetime(2024,1,1),
    catchup=False
) as dag:

    @task(outlets=[my_file_1])
    def update_dataset():
        with open(my_file_1.uri, "a+") as f:
            f.write("producer update")
    
    update_dataset()
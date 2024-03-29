from airflow import DAG, Dataset
from airflow.decorators import task
from datetime import datetime
from include.datasets import MY_FILE

with DAG(
    dag_id = "producer_test",
    schedule='@daily',
    start_date=datetime(2024,1,1),
    catchup=False,
    tags=['dataset']
) as dag:

    @task(outlets=[MY_FILE])
    def update_dataset():
        with open(MY_FILE.uri, "w") as f:
            f.write(":producer update")
    
    update_dataset()
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
from os.path import dirname, abspath
sys.path.append(dirname(dirname(abspath(__file__))))

from plugins.hooks.elasticsearch.elastic_hook import ElasticHook 
 
def _print_es_info():
    hook=ElasticHook()
    print(hook.info)
 
with DAG('elastic_dag', start_date=datetime(2022, 1, 1), schedule_interval='@daily', catchup=False) as dag:
 
    print_es_info = PythonOperator(
        task_id='print_es_info',
        python_callable=_print_es_info
    )
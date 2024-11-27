import os, sys
from os.path import dirname, abspath
sys.path.append(dirname(abspath(__file__)))
from dag_generator import SharepointDAGFactory
from yaml_config import load_yaml_config
from airflow.operators.python import PythonVirtualenvOperator


# Путь к YAML-файлу
CONFIG_FILE_NAME = "dag_config.yaml"
current_dir = os.path.dirname(os.path.abspath(__file__))
PATH_TO_YAML = SharepointDAGFactory.find_yaml_folder(current_dir, CONFIG_FILE_NAME)
dag_configs = load_yaml_config(PATH_TO_YAML)

# Генерация DAG'ов
for dag_config in dag_configs["dags"]:
    try:
        dag = SharepointDAGFactory.build_dag(dag_config)
        globals()[dag.dag_id] = dag
        print(f"DAG {dag.dag_id} created successful!")
    except Exception as e:
        print(f"Error during creating DAG {dag_config['params']['output']['minio_bucket'].split('-')[0]}_{dag_config['name']}: {e}")

import os, sys
import yaml
# from os.path import dirname, abspath
# sys.path.append(dirname(dirname(dirname(abspath(__file__)))))
# from dag_generator import SharepointDAGFactory
# from yaml_config import load_yaml_config
# from draft.task_factory import TaskFactory
from airflow.operators.python import PythonVirtualenvOperator
import datetime
from airflow import DAG


REQUIREMENTS=[
                "deltalake",
                "pandas",
                "pyarrow",
                "futures",
                "requests",
                "duckdb",
                "retry",
                "Office365-REST-Python-Client==2.5.12",
                "openpyxl"
            ]


def task_load():
    print(f"LOAD DATA")

def task_optimize():
    print(f"OPTIMIZE DATA")

def create_load_task():
    return task_load

def create_optimize_task():
    return task_optimize

TASK_FUNCTIONS = {
    "load": create_load_task,
    "optimize": create_optimize_task
}

class SharepointDAGFactory:
    @staticmethod
    def build_dag(dag_config):

        team_name = dag_config["params"]["output"]["minio_bucket"].split("-")[0]
        dag_id = f'mirroring_sharepoint_{team_name}__{dag_config["name"]}'
        dag = DAG(
            dag_id=dag_id,
            schedule=dag_config["schedule_interval"],
            default_args=dag_config["default_args"],
            catchup=False,
            tags=['dgoit', 'sharepoint', 'mirroring', 'ingest', f'team:{team_name}'] + dag_config.get("tags", []),
            start_date=datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc)
        )

        task_load_operator = PythonVirtualenvOperator(
            task_id="load_task",
            python_callable=TASK_FUNCTIONS["load"](),
            requirements=REQUIREMENTS,
            system_site_packages=False,
            # pool=f'j-palefat-{team_name}',
            dag=dag
        )

        task_optimize_operator = PythonVirtualenvOperator(
            task_id="optimize_task",
            python_callable=TASK_FUNCTIONS["optimize"](),
            requirements=REQUIREMENTS,
            system_site_packages=False,
            # pool=f'j-palefat-{team_name}',
            dag=dag
        )

        task_load_operator >> task_optimize_operator

        return dag


def load_yaml_config(file_path):
    with open(file_path, "r") as file:
        return yaml.safe_load(file)

# Путь к YAML-файлу
CONFIG_FILE_NAME = "dag_config.yaml"

current_dir = os.path.dirname(os.path.abspath(__file__))

def find_yaml_folder(current_dir, CONFIG_FILE_NAME):
    for dirpath, _, filenames in os.walk(current_dir):
        if CONFIG_FILE_NAME in filenames:
            return os.path.join(dirpath, CONFIG_FILE_NAME)
    raise FileNotFoundError(f"File {CONFIG_FILE_NAME} not found into directory {current_dir} and its subfolder.")


PATH_TO_YAML = find_yaml_folder(current_dir, CONFIG_FILE_NAME)
dag_configs = load_yaml_config(PATH_TO_YAML)

# Генерация DAG'ов
for dag_config in dag_configs["dags"]:
    try:
        dag = SharepointDAGFactory.build_dag(dag_config)
        globals()[dag.dag_id] = dag
        print(f"DAG {dag.dag_id} created successful!")
    except Exception as e:
        print(f"Error during creating DAG {dag_config['params']['output']['minio_bucket'].split('-')[0]}_{dag_config['name']}: {e}")

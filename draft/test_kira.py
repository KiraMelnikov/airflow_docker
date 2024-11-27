from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
import yaml
import os

CONFIG_FILE_NAME = "dag_config.yaml"

current_dir = os.path.dirname(os.path.abspath(__file__))

def find_yaml_folder(current_dir, CONFIG_FILE_NAME):
    for dirpath, _, filenames in os.walk(current_dir):
        if CONFIG_FILE_NAME in filenames:
            return os.path.join(dirpath, CONFIG_FILE_NAME)
    raise FileNotFoundError(f"Файл {CONFIG_FILE_NAME} не найден в директории {current_dir} и её подпапках.")

PATH_TO_YAML = find_yaml_folder(current_dir, CONFIG_FILE_NAME)

# Функция для парсинга YAML
def parse_yaml(file_path):
    with open(file_path, "r") as file:
        return yaml.safe_load(file)

# Функция для создания DAG
def create_dag(dag_id, schedule, default_args, tasks):
    dag = DAG(
        dag_id=dag_id,
        default_args=default_args,
        schedule_interval=schedule,
        catchup=False,
    )

    with dag:
        for task in tasks:
            BashOperator(
                task_id=task["task_id"],
                bash_command=task["bash_command"],
            )

    return dag

# Парсинг YAML для получения конфигурации DAG'ов
dag_config = parse_yaml(PATH_TO_YAML)

# Общие аргументы для всех DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "start_date": days_ago(1),
}

# Динамическая генерация DAG'ов
for dag_info in dag_config["dags"]:
    dag_id = dag_info["name"]
    schedule = dag_info["schedule_interval"]
    tasks = dag_info["tasks"]

    # Добавление DAG в контекст Airflow через globals()
    globals()[dag_id] = create_dag(dag_id, schedule, default_args, tasks)

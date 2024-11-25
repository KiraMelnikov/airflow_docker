from airflow import DAG
from airflow.utils.dates import days_ago
from logic.task_builder import build_task
import yaml

# Путь к YAML-файлу
CONFIG_FILE_PATH = "dags/config/dag_config.yml"

# Считывание YAML-конфигурации
def parse_yaml(file_path):
    with open(file_path, "r") as file:
        return yaml.safe_load(file)

# Парсим конфигурацию
dag_config = parse_yaml(CONFIG_FILE_PATH)

# Общие аргументы для DAG'ов
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "start_date": days_ago(1),
}

# Генерация DAG'ов
for dag_info in dag_config["dags"]:
    dag_id = dag_info["name"]
    schedule = dag_info["schedule_interval"]

    # Создаем DAG
    dag = DAG(
        dag_id=dag_id,
        default_args=default_args,
        schedule_interval=schedule,
        catchup=False,
    )

    # Добавляем задачи в DAG
    for task_config in dag_info["tasks"]:
        build_task(task_config, dag)

    # Регистрируем DAG в Airflow
    globals()[dag_id] = dag

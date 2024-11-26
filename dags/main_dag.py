from loader.dag_generator import SharepointDAGFactory
from utils.yaml_config import load_yaml_config

# Путь к YAML-файлу
CONFIG_FILE_PATH = "dags/config/dag_config.yml"

dag_configs = load_yaml_config(CONFIG_FILE_PATH)

# Генерация DAG'ов
for dag_config in dag_configs["dags"]:
    dag = SharepointDAGFactory.build_dag(dag_config)
    globals()[dag.dag_id] = dag

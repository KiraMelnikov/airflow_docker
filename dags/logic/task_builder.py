from airflow.operators.bash import BashOperator
from airflow.models import DAG

# Функция для создания задачи
def build_task(task_config, dag):
    task_type = task_config["type"]
    params = task_config["params"]

    if task_type == "bash":
        return BashOperator(
            task_id=task_config["task_id"],
            bash_command=params["bash_command"],
            dag=dag,
        )
    else:
        raise ValueError(f"Неизвестный тип задачи: {task_type}")

import datetime
from airflow import DAG
from airflow.operators.python import PythonVirtualenvOperator
from modules.task_builder import TASK_FUNCTIONS
from utils.requirements import REQUIREMENTS

class SharepointDAGFactory:
    @staticmethod
    def build_dag(dag_config):
        dag = DAG(
            dag_id=dag_config["name"],
            schedule_interval=dag_config["schedule_interval"],
            default_args=dag_config["default_args"],
            catchup=False,
            tags=dag_config.get("tags", []),
        )
        
        for task_config in dag_config["tasks"]:
            task_type = task_config["type"]
            task_func = TASK_FUNCTIONS.get(task_type)

            if not task_func:
                raise ValueError(f"Неизвестный тип задачи: {task_type}")

            PythonVirtualenvOperator(
                task_id=task_config["task_id"],
                python_callable=task_func,
                op_kwargs=task_config["params"],
                requirements=REQUIREMENTS,  # Зависимости окружения
                system_site_packages=False,
                dag=dag,
            )
        
        return dag


"""
    @staticmethod
    def build_dag(dag_config):
        dag_id = dag_config["name"]
        schedule = dag_config["schedule_interval"]
        default_args = dag_config.get("default_args", { 'owner': 'k.melnikov',
                                                        'email': ['k.melnikov@temabit.com'],
                                                        'email_on_retry': True,
                                                        'depends_on_past': False}
                                        )
        list_tags = ['sharepoint', 'dgoit', 'ingest', 'mirroring'] + dag_config["tags"]
        dag_args = {
            'concurrency': 1,
            'max_active_runs': 1,
            'start_date': datetime.datetime(2024, 1, 1, tz="Europe/Kiev"),
            'schedule_interval': schedule,
            'catchup': False,
            'tags': list_tags,
            "access_control": {
                "palefat-de_user": {"can_dag_read", "can_dag_edit"},
                "palefat-PredictionTeam_user": {"can_dag_read", "can_dag_edit"},
                "palefat-dgoit_user": {"can_dag_read", "can_dag_edit"},
            }
        }
        
        # Создание DAG
        dag = DAG(
            dag_id=dag_id,
            default_args=default_args,
            **dag_args
        )
        
        # Добавление задач
        for task_config in dag_config["tasks"]:
            task_type = task_config["type"]
            task_creator = TASK_CREATORS.get(task_type)
            
            if not task_creator:
                raise ValueError(f"Неизвестный тип задачи: {task_type}")
            
            task_creator(task_config, dag)
        
        return dag
"""
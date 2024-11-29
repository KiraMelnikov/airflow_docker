import datetime
from airflow import DAG
from airflow.operators.python import PythonVirtualenvOperator
import sys, os
from os.path import dirname, abspath
sys.path.append(dirname(abspath(__file__)))
from task_factory import TASK_FUNCTIONS
from requirements import REQUIREMENTS

class SharepointDAGFactory:
    @staticmethod
    def build_dag(dag_config):

        column_names = dag_config['params']['details']['column_names']
        column_types = dag_config['params']['details']['column_types']
        minio_bucket = dag_config["params"]["output"]["minio_bucket"]
        minio_path = dag_config["params"]["output"]["minio_path"]
        sharepoint_chanel = dag_config["params"]["input"]["sharepoint_chanel"]
        sharepoint_folder_name = dag_config["params"]["input"]["sharepoint_folder_name"]
        sharepoint_file_name = dag_config["params"]["input"]["sharepoint_file_name"]
        date_columns = dag_config['params']['details']['date_columns']
        datetime_columns = dag_config['params']['details']['datetime_columns']
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
            op_kwargs={
                "column_names": column_names,
                "column_types": column_types,
                "minio_bucket": minio_bucket,
                "minio_path": minio_path,
                "sharepoint_chanel": sharepoint_chanel,
                "sharepoint_folder_name": sharepoint_folder_name,
                "sharepoint_file_name": sharepoint_file_name,
                "datetime_columns": datetime_columns,
                "date_columns": date_columns
            },
            requirements=REQUIREMENTS,
            system_site_packages=False,
            # pool=f'j-palefat-{team_name}',
            dag=dag
        )

        task_optimize_operator = PythonVirtualenvOperator(
            task_id="optimize_task",
            python_callable=TASK_FUNCTIONS["optimize"](),
            op_kwargs={
                "column_names": column_names,
                "column_types": column_types,
                "minio_bucket": minio_bucket,
                "minio_path": minio_path,
                "sharepoint_chanel": sharepoint_chanel,
                "sharepoint_folder_name": sharepoint_folder_name,
                "sharepoint_file_name": sharepoint_file_name,
                "datetime_columns": datetime_columns,
                "date_columns": date_columns
            },
            requirements=REQUIREMENTS,
            system_site_packages=False,
            # pool=f'j-palefat-{team_name}',
            dag=dag
        )

        task_load_operator >> task_optimize_operator

        return dag

    @staticmethod
    def find_yaml_folder(current_dir, CONFIG_FILE_NAME):
        for dirpath, _, filenames in os.walk(current_dir):
            if CONFIG_FILE_NAME in filenames:
                return os.path.join(dirpath, CONFIG_FILE_NAME)
        raise FileNotFoundError(f"File {CONFIG_FILE_NAME} not found into directory {current_dir} and its subfolder.")
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
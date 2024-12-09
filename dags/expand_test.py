from datetime import datetime, timezone
from airflow import DAG
from airflow.operators.python import PythonVirtualenvOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.task_group import MappedTaskGroup


def load_data_to_minio(column_names, column_types, minio_bucket, minio_path,
                       sharepoint_channel, sharepoint_folder_name, sharepoint_file_name,
                       datetime_columns=None, date_columns=None):
    import pandas as pd
    import sys
    from datetime import datetime, timezone

    sys.path.append("/opt/airflow/dags/sharepoint/ingestor")
    from input_details import InputDetailsLoader
    from loader.loader_factory import LoaderFactory

    pd.set_option('display.width', 320)
    pd.set_option("display.max_columns", 20)
    pd.set_option('max_colwidth', None)

    def convert_column_types(columns):
        type_mapping = {
            "str": str,
            "float": float,
            "int": int,
            "bool": bool,
            "Int64": "Int64"
        }
        return {col: type_mapping.get(dtype, str) for col, dtype in columns.items()}

    def change_column_types(df, columns, data_type):
        columns = columns or []
        if data_type == 'date':
            for col in columns:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors='coerce', dayfirst=True).dt.date
        elif data_type == 'datetime':
            for col in columns:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors='coerce', dayfirst=True).dt.tz_localize('UTC').dt.floor(
                        's')
        return df

    current_datetime = datetime.now(tz=timezone.utc).replace(microsecond=0)
    input_details = InputDetailsLoader(
        data_source="Sharepoint",
        login="j-dgoit-datasync@temabit.com",
        password="5Na*MYdPwHeLgs_6m6Ab",
        channel=sharepoint_channel,
        folder_name=sharepoint_folder_name,
        file_name=sharepoint_file_name,
        column_names=column_names,
        column_types=convert_column_types(column_types)
    )

    loader = LoaderFactory.create_loader(input_details)
    df = loader.get_data()
    df = df.iloc[:, :16]
    df = df[df['project_name'].notna()].reset_index(drop=True)
    df = change_column_types(df, datetime_columns, 'datetime')
    df['_platform_ingested_at'] = current_datetime
    print(df)


def optimize_data(column_names, column_types, minio_bucket, minio_path,
                       sharepoint_channel, sharepoint_folder_name, sharepoint_file_name,
                       datetime_columns=None, date_columns=None):
    print(f'Optimizing: {minio_bucket}/{minio_path}')
    print("Optimization task executed.")

def sanitize_id(name):
    return name.split('.')[0].replace(' ', '_').replace('-', '_').lower()

schedule = {"schedule": "0 8 * * *"}
team_name = {"team":"dgoit"}
tags = {"tags":["sales"]}
owner = {"owner":"k.melnikov"}
emails = ['k.melnikov@temabit.com']
config_params = [
    {
        "column_names": [ "project_name", "code_prm", "audit_date", "gen_score", "gen_total", "blocked", "app_score",
                        "app_total", "blocked2", "infr_score", "infr_total", "blocked3", "audit_score", "audit_total",
                        "blocked4", "result_score" ],
        "column_types": {"project_name": str, "code_prm": str, "gen_score": float, "gen_total": float,
                    "blocked": "Int64", "app_score": float, "app_total": float, "blocked2": "Int64", "infr_score": float,
                    "infr_total": float, "blocked3": "Int64", "audit_score": float, "audit_total": float,
                    "blocked4": "Int64",
                    "result_score": float},
        "minio_bucket": "dgoit-readonly",
        "minio_path": "sharepoint/cyber_security/check_list",
        "sharepoint_channel": "CyberSecurity",
        "sharepoint_folder_name": "check_list",
        "sharepoint_file_name": "Security Check List_results_.xlsx",
        "datetime_columns": ["audit_date"],
        "date_columns": []
    },
    {
        "column_names": [ "project_name", "code_prm", "audit_date", "gen_score", "gen_total", "blocked", "app_score",
                        "app_total", "blocked2", "infr_score", "infr_total", "blocked3", "audit_score", "audit_total",
                        "blocked4", "result_score" ],
        "column_types": {"project_name": str, "code_prm": str, "gen_score": float, "gen_total": float,
                    "blocked": "Int64", "app_score": float, "app_total": float, "blocked2": "Int64", "infr_score": float,
                    "infr_total": float, "blocked3": "Int64", "audit_score": float, "audit_total": float,
                    "blocked4": "Int64",
                    "result_score": float},
        "minio_bucket": "dgoit-readonly",
        "minio_path": "sharepoint/sales/sales_online",
        "sharepoint_channel": "CyberSecurity",
        "sharepoint_folder_name": "check_list_test",
        "sharepoint_file_name": "Sales_Online report.xlsx",
        "datetime_columns": ["audit_date"],
        "date_columns": []
    }
]

REQUIREMENTS = [
    "deltalake==0.19.1",
            "pandas==2.2.0",
            "pyarrow==17.0.0",
            "futures",
            "requests==2.31.0",
            "duckdb==1.0.0",
            "retry",
            "Office365-REST-Python-Client==2.5.12",
            "openpyxl",
            "boto3"
]


with DAG(
        dag_id=f"sharepoint_mirroring_{sanitize_id(team_name.get('team', ' '))}",
        schedule=schedule.get('schedule', '@daily'),
        default_args = {
            'owner': owner.get('owner', 'airflow'),
            'email': list({email for email in emails}),
            'email_on_retry': True,
            'depends_on_past': False,
            'retries': 1,
            'retry_delay': 5
        },
        catchup=False,
        start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
        tags=['dgoit', 'sharepoint', 'mirroring', 'ingest', f'team:{team_name.get("team", " ")}'] + tags.get("tags", []),
        # access_control={
        #     "palefat-de_user": {"can_read", "can_edit"},
        #     "palefat-PredictionTeam_user": {"can_read", "can_edit"},
        #     "palefat-dgoit_user": {"can_read", "can_edit"}
        # }
) as dag:

    for param in config_params:

        with TaskGroup(group_id=f"mirroring__{sanitize_id(param['sharepoint_file_name'])}"
        ) as dynamic_group:

            load_task = PythonVirtualenvOperator.partial(
                # task_id=f"load_from_sharepoint__{sanitize_id(param['sharepoint_file_name'])}",
                task_id="load_from_sharepoint",
                python_callable=load_data_to_minio,
                requirements=REQUIREMENTS,
                system_site_packages=False,
                map_index_template="{{ task.op_kwargs['sharepoint_file_name'].split('.')[0] | lower | replace(' ', '_') | replace('-', '_') }}"
            ).expand(
                op_kwargs=[{
                    "column_names": param["column_names"],
                    "column_types": param["column_types"],
                    "minio_bucket": param["minio_bucket"],
                    "minio_path": param["minio_path"],
                    "sharepoint_channel": param["sharepoint_channel"],
                    "sharepoint_folder_name": param["sharepoint_folder_name"],
                    "sharepoint_file_name": param["sharepoint_file_name"],
                    "datetime_columns": param.get("datetime_columns", []),
                    "date_columns": param.get("date_columns", [])
                }]
            )

            optimize_task = PythonVirtualenvOperator.partial(
                # task_id=f"optimize_minio__{sanitize_id(param['sharepoint_file_name'])}",
                task_id="optimize_in_minio",
                python_callable=optimize_data,
                requirements=REQUIREMENTS,
                system_site_packages=False,
                map_index_template="{{ task.op_kwargs['sharepoint_file_name'].split('.')[0] | lower | replace(' ', '_') | replace('-', '_') }}"
            ).expand(
                op_kwargs=[{
                    "column_names": param["column_names"],
                    "column_types": param["column_types"],
                    "minio_bucket": param["minio_bucket"],
                    "minio_path": param["minio_path"],
                    "sharepoint_channel": param["sharepoint_channel"],
                    "sharepoint_folder_name": param["sharepoint_folder_name"],
                    "sharepoint_file_name": param["sharepoint_file_name"],
                    "datetime_columns": param.get("datetime_columns", []),
                    "date_columns": param.get("date_columns", [])
                }]
            )

            load_task >> optimize_task

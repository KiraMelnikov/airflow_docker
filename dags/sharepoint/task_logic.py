import os


def task_load(column_names:list, column_types:dict, minio_bucket:str, minio_path:str, sharepoint_chanel:str,
              sharepoint_folder_name:str, sharepoint_file_name:str, datetime_columns:str=None, date_columns:str=None) -> None:

    import pandas as pd
    import sys, os
    from datetime import datetime, timezone
    sys.path.append("/opt/airflow/dags/sharepoint/ingestor")
    from input_details import InputDetailsLoader
    from loader.loader_factory import LoaderFactory
    pd.set_option('display.width', 320)
    pd.set_option("display.max_columns", 20)
    pd.set_option('max_colwidth', None)

    def convert_type_columns(columns:dict) -> dict:
        type_mapping = {
            "str": str,
            "float": float,
            "int": int,
            "bool": bool,
            "Int64": "Int64"
        }

        return {
            column: type_mapping[type_str]
            for column, type_str in columns.items()
        }

    def change_column_types(df:pd.DataFrame, columns:list=None, data_type:str=None) -> pd.DataFrame:
        """
        Преобразует указанные колонки DataFrame в заданные типы.

        :param df: pd.DataFrame, исходный DataFrame.
        :param columns: list, список колонок для преобразования.
        :param data_type: str, тип входной колонки (дата или датавремя).
        :return: pd.DataFrame, DataFrame с преобразованными колонками.
        """
        columns = columns or []

        if data_type == 'date':
            for col in columns:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors='coerce', dayfirst=True).dt.date
                else:
                    print(f"Warning: Column '{col}' not found in DataFrame")
        elif data_type == 'datetime':
            for col in columns:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors='coerce', dayfirst=True)
                    df[col] = df[col].dt.tz_localize('UTC').dt.floor('s')
                else:
                    print(f"Warning: Column '{col}' not found in DataFrame")

        return df


    current_datetime = datetime.now(tz=timezone.utc).replace(microsecond=0)
    login = "j-dgoit-datasync@temabit.com"
    password = "5Na*MYdPwHeLgs_6m6Ab"
    chanel = sharepoint_chanel
    folder_name = sharepoint_folder_name
    file_name = sharepoint_file_name
    column_names = column_names
    column_types = convert_type_columns(column_types)

    input_details = InputDetailsLoader(data_source="Sharepoint",  # static name for this kind of ingest
                                       login=login,  # Vault static login
                                       password=password,  # Vault password
                                       chanel=chanel,  # chanel for each palefat team
                                       folder_name=folder_name,  # some folder with files
                                       file_name=file_name,  # target file name
                                       column_names=column_names,  # list of column names for dataframe
                                       column_types=column_types)


    print("========================SET-UP LOADER/READER/WRITER========================")
    loader = LoaderFactory.create_loader(input_details)

    df = loader.get_data()

    df = df.iloc[0:, 0:16]
    df = df[df['project_name'].notna()].reset_index(drop=True)
    df = change_column_types(df, datetime_columns, 'datetime')
    df['_platform_ingested_at'] = current_datetime
    print(df)


def task_optimize(column_names:list, column_types:dict, minio_bucket:str, minio_path:str, sharepoint_chanel:str,
              sharepoint_folder_name:str, sharepoint_file_name:str, datetime_columns:str=None, date_columns:str=None) -> None:
    print(f"OPTIMIZE DATA:")
    print(column_names)
    print(column_types)
    print(minio_bucket)
    print(minio_path)
    print(sharepoint_chanel)
    print(sharepoint_folder_name)
    print(sharepoint_file_name)


# task_load(column_names=["project_name", "code_prm", "audit_date", "gen_score", "gen_total", "blocked", "app_score",
#                         "app_total", "blocked2", "infr_score", "infr_total", "blocked3", "audit_score", "audit_total",
#                         "blocked4", "result_score"],
#           column_types={"project_name": 'str', "code_prm": 'str', "gen_score": 'float', "gen_total": 'float',
#                     "blocked": "Int64", "app_score": 'float', "app_total": 'float', "blocked2": "Int64", "infr_score": 'float',
#                     "infr_total": 'float', "blocked3": "Int64", "audit_score": 'float', "audit_total": 'float',
#                     "blocked4": "Int64",
#                     "result_score": 'float'}, minio_bucket="None", minio_path="None", sharepoint_chanel="CyberSecurity",
#           sharepoint_folder_name="check_list", sharepoint_file_name="Security Check List_results_.xlsx",
#           datetime_columns=['audit_date'])
from deltalake import DeltaTable
from reader.s3_reader_interface import S3Reader
from utils import build_select_query, build_where_condition, get_kwarg_or_default
import pandas as pd
import duckdb


class S3ReaderWithJoin(S3Reader):
    def __init__(self, kwargs: dict):
        self.ACCESS_KEY_ID = kwargs['ACCESS_KEY_ID']
        self.SECRET_ACCESS_KEY = kwargs['SECRET_ACCESS_KEY']
        self.endpoint_url = kwargs['endpoint_url']
        self.region = kwargs['region']
        self.allow_http = kwargs['allow_http']
        self.AWS_S3_ALLOW_UNSAFE_RENAME = kwargs['aws_s3_allow_unsafe_rename']
        self.SESSION_TOKEN = get_kwarg_or_default('SESSION_TOKEN',kwargs,None)
        self.storage_options = {}

    def get_minio_storage_options(self):
        self.storage_options = {'ACCESS_KEY_ID': self.ACCESS_KEY_ID,
                           'SECRET_ACCESS_KEY': self.SECRET_ACCESS_KEY,
                           'endpoint_url': self.endpoint_url,
                           'region': self.region,
                           'allow_http': self.allow_http,
                           'AWS_S3_ALLOW_UNSAFE_RENAME': self.AWS_S3_ALLOW_UNSAFE_RENAME}

        if self.SESSION_TOKEN:
            self.storage_options.update({'session_token': self.SESSION_TOKEN})

    def read(self, bucket_1, bucket_2, source_path_1, source_path_2, sql) -> list:
        self.get_minio_storage_options()
        s3_path_1 = f"s3://{bucket_1}/{source_path_1}"
        s3_path_2 = f"s3://{bucket_2}/{source_path_2}"
        deltaTanle_1 = DeltaTable(s3_path_1, storage_options=self.storage_options)
        deltaTanle_2 = DeltaTable(s3_path_2, storage_options=self.storage_options)
        tempTable_audit   = duckdb.arrow(deltaTanle_1.to_pyarrow_dataset())
        tempTable_filials = duckdb.arrow(deltaTanle_2.to_pyarrow_dataset())
        dt = duckdb.query(sql).to_df()
        return dt.values.tolist()







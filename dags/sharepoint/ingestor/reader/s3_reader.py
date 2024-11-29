from deltalake import DeltaTable
from reader.s3_reader_interface import S3Reader
from utils import build_select_query, build_where_condition, get_kwarg_or_default
import pandas as pd
import duckdb
from typing import (
        Optional,
        Union,
        Any,
        List,
        Tuple,
        Set,
        Dict
    )

class S3BaseReader(S3Reader):
    """Base class"""
    def __init__(self, kwargs: dict) -> None:
        self.bucket = kwargs["bucket"]
        self.source_path = kwargs["source_path"]
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
    
    def run(self):
        pass


class S3DataReader(S3BaseReader):

    def __init__(self, kwargs: dict):
        super().__init__(kwargs)
        self.select_query = build_select_query("column_list", kwargs)
        self.where_condition = build_where_condition('where_condition', kwargs)
        self.order_by = kwargs['order_by']
        self.column_list = kwargs["column_list"]
        
    def get_date_range(self,start_date, end_date,format) -> list[str]:
        date_list = []
        for date in pd.date_range(start=start_date,end=end_date,freq='D'):
            date_list.append(date.strftime(format))
        return date_list

    def read(self) -> list:
        self.get_minio_storage_options()
        s3_path = f"s3://{self.bucket}/{self.source_path}"
        deltaTanle = DeltaTable(s3_path, storage_options=self.storage_options)
        column_count = len(self.column_list)
        tempTable = duckdb.arrow(deltaTanle.to_pyarrow_dataset())
        if self.where_condition:
            sql_build = f'{self.select_query} tempTable {self.where_condition}'
        else:
            sql_build = f'{self.select_query} tempTable'

        if self.order_by:
            sql_build = f"{sql_build} {self.order_by}"
        dt = duckdb.query(sql_build).to_df()
        if column_count == 1 :
            return [x for x, in dt.values]
        else:
            return dt.values.tolist()
        
class S3DeltaReader(S3BaseReader):

    def __init__(self, kwargs: dict):
        super().__init__(kwargs)
        self.where_condition = build_where_condition('where_condition', kwargs)

    def read(self, isPandas: Optional[Union[str, int]]= None, version: Optional[int] = None) -> pd.DataFrame|DeltaTable:
        """Reading the data from S3 path and return a DeltaTable or Pandas DataFrame depends of :params 'isPandas'
            where_condition: only returnthe pandas df"""
        print('\nUSED READER\n')

        self.get_minio_storage_options()
        s3_path = f"s3://{self.bucket}/{self.source_path}"
     
        if isPandas:
            if version:
                deltaTable = DeltaTable(s3_path, storage_options=self.storage_options, version=version)
            else:
                deltaTable = DeltaTable(s3_path, storage_options=self.storage_options)
            tempTable = duckdb.arrow(deltaTable.to_pyarrow_dataset())
            if self.where_condition:
                sql_build = f'select * from tempTable {self.where_condition}'
            else:
                sql_build = f'select * from tempTable'
            df = duckdb.query(sql_build).to_df()
            return df
        else:
            if version:
                return DeltaTable(s3_path, storage_options=self.storage_options, version=version)
            else:
                return DeltaTable(s3_path, storage_options=self.storage_options)
        





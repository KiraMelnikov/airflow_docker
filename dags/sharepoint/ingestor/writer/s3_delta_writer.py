from datetime import datetime
from deltalake._internal import TableNotFoundError
import pandas as pd
import pyarrow as pa
from deltalake import write_deltalake, DeltaTable, Schema
from deltalake.schema import _convert_pa_schema_to_delta
from writer.writer_interface import Writer
from typing import (
        Optional,
        Union,
        Any,
        List,
        Tuple,
        Set,
        Dict
    )
from utils import get_kwarg_or_default

CHECKPOINT_INTERVAL = 10


class S3DeltaWriter(Writer):
    def __init__(self, kwargs):
        self.add_metadata = get_kwarg_or_default('add_metadata', kwargs, False)
        self.delta_write_mode = kwargs['delta_write_mode']
        self.s3_path = f"s3://{kwargs['bucket']}/{kwargs['key']}"
        self.storage_options = {'ACCESS_KEY_ID': kwargs['ACCESS_KEY_ID'],
                                'SECRET_ACCESS_KEY': kwargs['SECRET_ACCESS_KEY'],
                                'endpoint_url': kwargs['endpoint_url'],
                                'region': kwargs['region'],
                                'allow_http': kwargs['allow_http'],
                                'AWS_S3_ALLOW_UNSAFE_RENAME': kwargs['aws_s3_allow_unsafe_rename']}

        session_token = 'SESSION_TOKEN'
        if session_token in kwargs:
            self.storage_options.update({session_token: kwargs[session_token]})
        self.delta_table = self.__try_init_delta_table()

    @staticmethod
    def __add_metadata(df: pd.DataFrame) -> None:
        df['_platform_ingested_at'] = datetime.now()

    def write(self, df: pd.DataFrame) -> None:
        assert isinstance(df, pd.DataFrame)
        if self.add_metadata:
            self.__add_metadata(df)
        pa_table = pa.Table.from_pandas(df=df)
        if self.delta_table is None:
            self.__create_delta_table(Schema.from_pyarrow(_convert_pa_schema_to_delta(pa_table.schema)))
        if pa_table.shape[0] == 0:
            raise ValueError("DataFrame is empty")
        write_deltalake(table_or_uri=self.delta_table, data=pa_table, mode=self.delta_write_mode)
        self.__create_checkpoint()

    def write_temp(self, dataframe: pd.DataFrame) -> None:
        pa_table = pa.Table.from_pandas(df=dataframe)
        write_deltalake(table_or_uri=self.s3_path, data=pa_table,
                        storage_options=self.storage_options, mode=self.delta_write_mode)

    def __try_init_delta_table(self):
        delta_table = None
        try:
            delta_table = DeltaTable(table_uri=self.s3_path,
                                     storage_options=self.storage_options)
        except TableNotFoundError:
            print('Table not found. It will be created on the first write.')
        return delta_table


    def __create_delta_table(self, delta_schema):
        self.delta_table = DeltaTable.create(table_uri=self.s3_path,
                                             schema=delta_schema,
                                             storage_options=self.storage_options)

    def __create_checkpoint(self):
        if self.delta_table.version() % CHECKPOINT_INTERVAL == 0:
            self.delta_table.create_checkpoint()
            print('Checkpoint created.')

    def merge(self, target: DeltaTable, source: pd.DataFrame, target_alias: str,
              source_alias: str, predicate: str, with_del_not_matched: Any = None ) -> DeltaTable:
        """
            Merging 2 tables like DeltaTable and DataFrame, then to return has merged DeltaTable.
            There is a possibility of choose to delete rows when they not matched from source or not.

            Args:
                predicate: consist from SQL syntax
        """
        print('\nUSED MERGE\n')
        if not with_del_not_matched:
            (
            target.merge(
                source=source,
                predicate=predicate,
                target_alias=target_alias,
                source_alias=source_alias
            )
            .when_matched_update_all()
            .when_not_matched_insert_all()
            .execute()
            )
            return target
        else:
            (
            target.merge(
                source=source,
                predicate=predicate,
                target_alias=target_alias,
                source_alias=source_alias
            )
            .when_matched_update_all()
            .when_not_matched_insert_all()
            .when_not_matched_by_source_delete()
            .execute()
            )
            return target

    def optimize(self):
        DeltaTable(self.s3_path, storage_options=self.storage_options).optimize

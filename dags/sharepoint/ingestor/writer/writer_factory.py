from output_details import OutputDetails
from utils import check_required_kwargs
from writer.local_csv_copy_writer import LocalCsvCopyWriter
from writer.local_delta_writer import LocalDeltaWriter
from writer.s3_csv_copy_writer import S3CsvCopyWriter
from writer.s3_delta_writer import S3DeltaWriter
from writer.writer_interface import Writer
import pandas

DELTA_REQUIRED_ARGS = ['delta_write_mode']

LOCAL_REQUIRED_ARGS = ['local_path']
LOCAL_COPY_REQUIRED_ARGS = ['copy_local_path']

S3_REQUIRED_ARGS = ['ACCESS_KEY_ID', 'SECRET_ACCESS_KEY',
                    'endpoint_url', 'bucket', 'key', 'region', 'allow_http',
                    'aws_s3_allow_unsafe_rename']
S3_COPY_REQUIRED_ARGS = ['ACCESS_KEY_ID', 'SECRET_ACCESS_KEY',
                         'endpoint_url', 'copy_bucket', 'copy_key', 'region', 'allow_http',
                         'aws_s3_allow_unsafe_rename']


class WriterFactory:
    @staticmethod
    def create_writer(output_details: OutputDetails) -> Writer:
        if output_details.data_format == 'DELTA':
            check_required_kwargs(output_details.kwargs, DELTA_REQUIRED_ARGS)

        if output_details.destination == 'LOCAL':
            check_required_kwargs(output_details.kwargs, LOCAL_REQUIRED_ARGS)
            return LocalDeltaWriter(kwargs=output_details.kwargs)
        elif output_details.destination in ['S3', 'MINIO']:
            check_required_kwargs(output_details.kwargs, S3_REQUIRED_ARGS)
            return S3DeltaWriter(output_details.kwargs)
        else:
            raise RuntimeError(f"Writer for {output_details.destination} destination is not implemented")

    @staticmethod
    def create_copy_writer(output_details: OutputDetails):
        if output_details.destination == 'LOCAL':
            check_required_kwargs(output_details.kwargs, LOCAL_COPY_REQUIRED_ARGS)
            return LocalCsvCopyWriter(kwargs=output_details.kwargs)
        elif output_details.destination in ['S3', 'MINIO']:
            check_required_kwargs(output_details.kwargs, S3_COPY_REQUIRED_ARGS)
            return S3CsvCopyWriter(output_details.kwargs)
        else:
            raise RuntimeError(f"Copy writer for {output_details.destination} destination is not implemented")

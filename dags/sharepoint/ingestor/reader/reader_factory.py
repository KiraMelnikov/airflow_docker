from input_details import InputDetails
from reader.csv_reader import CsvReader
from reader.s3_reader import S3DataReader, S3DeltaReader
from reader.s3_read_with_join import S3ReaderWithJoin
from utils import check_required_kwargs

CSV_REQUIRED_ARGS = []
S3_REQUIRED_ARGS = ['ACCESS_KEY_ID', 'SECRET_ACCESS_KEY',
                    'endpoint_url', 'bucket', 'source_path', 'region', 'allow_http',
                    'aws_s3_allow_unsafe_rename']

class ReaderFactory:
    @staticmethod
    def create_reader(input_details: InputDetails):
        if input_details.data_format == 'CSV':
            check_required_kwargs(input_details.kwargs, CSV_REQUIRED_ARGS)
            return CsvReader(input_details.kwargs)
        elif input_details.data_format in ('ELCAR_API', 'AUDIT_RELOAD_API'):
            check_required_kwargs(input_details.kwargs, S3_REQUIRED_ARGS)
            return S3DataReader(input_details.kwargs)
        elif input_details.data_format in ('IT_AVAILABILITY'):
            check_required_kwargs(input_details.kwargs, S3_REQUIRED_ARGS)
            return S3DeltaReader(input_details.kwargs)
        elif input_details.data_format in ('AUDIT_API'):
            check_required_kwargs(input_details.kwargs, S3_REQUIRED_ARGS)
            return S3ReaderWithJoin(input_details.kwargs)
        elif input_details.data_format in ('SAP_API', 'WEIGHT_API'):
            return None
        else:
            raise NotImplementedError(f"Reader for {input_details.source} source is not implemented")


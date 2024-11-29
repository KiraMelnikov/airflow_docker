import sys
sys.path.append("/opt/airflow/dags/sharepoint/ingestor")
from loader.loader_interface import Loader
from loader.loader_requests_api import LoaderRequestsApi, LoaderSignRequest, LoaderAwsS3
from input_details import InputDetails
from utils import check_required_kwargs


S3_REQUIRED_ARGS = ['ACCESS_KEY_ID', 'SECRET_ACCESS_KEY',
                    'endpoint_url', 'bucket', 'source_path', 'region', 'allow_http',
                    'aws_s3_allow_unsafe_rename']
AWS_S3_ARGS = []
AWS_ARGS = ['ACCESS_KEY', 'SECRET_KEY', 'SESSION_TOKEN', 'region', 'service']
API_ARGS = []
SHAREPOINT_ARGS = ['login', 'password', 'chanel','folder_name', 'file_name', 'column_names', 'column_types']

class LoaderFactory(Loader):

    @staticmethod
    def create_loader(input_details: InputDetails):
        if input_details.data_source == 'API':
            args = AWS_ARGS if input_details.on_aws is True else API_ARGS
            if input_details.to_sign:
                check_required_kwargs(input_details.kwargs, args)
                return LoaderSignRequest(input_details.kwargs)
            else:
                check_required_kwargs(input_details.kwargs, args)
                return LoaderRequestsApi(input_details.kwargs)
        elif input_details.data_source == 'S3':
            check_required_kwargs(input_details.kwargs, S3_REQUIRED_ARGS)
            return LoaderAwsS3(input_details.kwargs)
        elif input_details.data_source == 'Sharepoint':
            from loader.loader_office365 import SharepointDataLoader
            check_required_kwargs(input_details.kwargs, SHAREPOINT_ARGS)
            return SharepointDataLoader(input_details.kwargs)
        else:
            raise ValueError(f"Unsupported data source: {input_details.data_source}")

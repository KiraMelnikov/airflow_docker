from typing import Union
import sys
import requests
from datetime import datetime
from io import StringIO, BytesIO
import pandas as pd
import boto3
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.credentials import Credentials
from retry import retry
sys.path.append("/opt/airflow/dags/sharepoint/ingestor")
from utils import get_kwarg_or_default

class LoaderRequestsApi:
    def __init__(self, kwargs):
        try:
            self.headers = get_kwarg_or_default("headers", kwargs, None)
            self.json_data = get_kwarg_or_default("json_data", kwargs, None)
        except:
            self.headers = get_kwarg_or_default("headers", kwargs.kwargs, None)
            self.json_data = get_kwarg_or_default("json_data", kwargs.kwargs, None)


    @retry(Exception, tries=3, delay=5, backoff=2)
    def LoaderRequests(self, url):
        if self.headers and self.json_data:
            response = requests.get(url, headers=self.headers, json_data=self.json_data)
        elif self.headers:
            response = requests.get(url, headers=self.headers)
        else:
            response = requests.get(url)
        print(f"response = {response}")
        return response


    @retry(Exception, tries=3, delay=5, backoff=2)
    def LoaderRequestsData(self, url):
        if self.headers and self.json_data:
            response = requests.get(url, headers=self.headers, data=self.json_data)
        elif self.headers:
            response = requests.get(url, headers=self.headers)
        else:
            response = requests.get(url)
        print(f"response = {response}")
        return response


    @retry(Exception, tries=3, delay=5, backoff=2)
    def LoaderRequestsPost(self, url, json=None):
        if json:
            response = requests.post(url, headers=self.headers, json=self.json_data)
        else:
            response = requests.post(url, headers=self.headers, data=self.json_data)
        return response


    def LoaderPdCsvFromRequests(self, url):
        try:
            if self.headers:
                response = requests.get(url, headers=self.headers)
            else:
                response = requests.get(url)
            return pd.read_csv(StringIO(response.text))
        except Exception as e:
            print(f"Error: {e}")
            return None


class LoaderSignRequest:
    """Loader for signing AWS request"""
    
    def __init__(self, kwargs) -> None:
        self.access_key = kwargs.get('ACCESS_KEY')
        self.secret_key = kwargs.get('SECRET_KEY')
        self.session_token = kwargs.get('SESSION_TOKEN')
        self.region = kwargs.get('region')
        self.service = kwargs.get('service')
        self.headers = kwargs.get('headers', None)
        self.params = kwargs.get('params')
        self.method = kwargs.get('request_method', 'get')
        self.meta = {'X-Amz-Date': datetime.now().strftime('%Y%m%dT%H%M%SZ')}

        if self.headers == None:
            self.__add_headers()
        else:
            self.__update_headers()
            

    def __update_headers(self):
        self.headers.update(self.meta)

    def __add_headers(self):
        self.headers = self.meta


    @retry(Exception, tries=3, delay=5, backoff=2)
    def sign_request(self, endpoint:str):
        """Add signature to aws request"""
        # Create AWS credentials object
        credentials = Credentials(self.access_key, self.secret_key, self.session_token)

        # Create a request object
        request = AWSRequest(
            method=self.method.upper(),
            url=endpoint,
            headers=self.headers,
            params=self.params
        )

        # Sign the request
        SigV4Auth(credentials, self.service, self.region).add_auth(request)

        # Convert the signed request to a format compatible with the requests library
        signed_headers = dict(request.headers.items())

        # Send the request using the requests library
        response = requests.request(
            method=self.method.upper(),
            url=endpoint,
            headers=signed_headers,
            params=self.params
        )
        return response

class LoaderAwsS3:
    """Loader for Amazon S3"""
    
    def __init__(self, creds:dict) -> None:
        self.__ACCESS_KEY  = get_kwarg_or_default('access_key', creds, default_value=None) 
        self.__SECRET_KEY = get_kwarg_or_default('secret_key', creds, default_value=None)
        self.__SESSION_TOKEN = get_kwarg_or_default('session_token', creds, default_value=None)
        self._BUCKET_NAME = get_kwarg_or_default('bucket_name', creds, default_value=None)
        self.FILE_PATH = get_kwarg_or_default('file_path', creds, default_value=None) 
        self.FILE_NAME = get_kwarg_or_default('file_name', creds, default_value=None)
        self.FILE = None

    def __create_client_S3(self):
        return boto3.client('s3', aws_access_key_id=self.__ACCESS_KEY,
                                  aws_secret_access_key=self.__SECRET_KEY,
                                  aws_session_token=self.__SESSION_TOKEN)

    def load_aws_s3_data(self):
        """Getting data from AWS S3"""
        s3_client = self.__create_client_S3()
        response = s3_client.get_object(Bucket=f'{self._BUCKET_NAME}', Key=f"{self.FILE_PATH }/{self.FILE_NAME}")
        self.FILE = response['Body'].read()
        return self


    def to_pd_csv(self, delimiter:str = ',') -> pd.DataFrame:
        file = BytesIO(self.FILE)
        df = pd.read_csv(file, encoding='utf8', delimiter=delimiter)
        return df


    def to_pd_excel(self, column_range:Union[tuple,list]=None, row_range:Union[tuple,list]=None) -> pd.DataFrame:
        file = BytesIO(self.FILE)
        df = pd.read_excel(file)
        row_start = row_range[0] if row_range and len(row_range) > 0 else None
        row_end = row_range[1] if row_range and len(row_range) > 1 else None
        column_start = column_range[0] if column_range and len(column_range) > 0 else None
        column_end = column_range[1] if column_range and len(column_range) > 1 else None
        if column_range and row_range:
            df = df.iloc[row_start:row_end, column_start:column_end]
        return df


    @retry(Exception, tries=3, delay=5, backoff=2)
    def get_list_buckets(self) -> list:
        """ Return the list of buckets from aws s3 """
        result_buckets = []
        s3_client = self.__create_client_S3()
        list_buckets = s3_client.list_buckets()
        for bucket in list_buckets['Buckets']:
            result_buckets.append(bucket['Name'])
        return result_buckets
import boto3

from tasks.ingestor.utils import get_kwarg_or_default
from tasks.ingestor.writer.writer_interface import Writer


class S3MultipartWriter(Writer):
    def __init__(self, kwargs: dict):
        session = boto3.session.Session(aws_access_key_id=kwargs['access_key_id'],
                                        aws_secret_access_key=kwargs['secret_access_key'],
                                        aws_session_token=get_kwarg_or_default('session_token', kwargs, None))
        self.s3_client = session.client(service_name='s3',
                                        endpoint_url=kwargs['endpoint_url'])
        self.bucket = kwargs['bucket']
        self.key = kwargs['key']
        self.upload_id = 0
        self.current_part_number = 1
        self.parts = []

    def __create_multipart_upload(self):
        print(f"Initializing multipart_upload for {self.bucket}/{self.key}")
        response = self.s3_client.create_multipart_upload(Bucket=self.bucket,
                                                          Key=self.key)
        self.upload_id = response['UploadId']

    def __complete_multipart_upload(self):
        print(f"Completing multipart_upload for {self.bucket}/{self.key}")
        response = self.s3_client.complete_multipart_upload(Bucket=self.bucket,
                                                            Key=self.key,
                                                            UploadId=self.upload_id,
                                                            MultipartUpload={
                                                                'Parts': self.parts
                                                            })
        print(f"S3 Object created at '{response['Bucket']}/{response['Key']}'")

    def __enter__(self):
        self.__create_multipart_upload()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.__complete_multipart_upload()

    def write(self, bytes_batch: bytes):
        if self.current_part_number == 1:
            self.__create_multipart_upload()
        response = self.s3_client.upload_part(Body=bytes_batch,
                                              Bucket=self.bucket,
                                              Key=self.key,
                                              PartNumber=self.current_part_number,
                                              UploadId=self.upload_id)
        self.parts.append({'ETag': response['ETag'],
                           'PartNumber': self.current_part_number})
        self.current_part_number += 1

import boto3

from writer.writer_interface import Writer


class S3CsvCopyWriter(Writer):
    def __init__(self, kwargs):
        session_token = 'SESSION_TOKEN'
        if session_token in kwargs:
            session = boto3.session.Session(aws_access_key_id=kwargs['ACCESS_KEY_ID'],
                                            aws_secret_access_key=kwargs['SECRET_ACCESS_KEY'],
                                            aws_session_token=kwargs[session_token])
        else:
            session = boto3.session.Session(aws_access_key_id=kwargs['ACCESS_KEY_ID'],
                                            aws_secret_access_key=kwargs['SECRET_ACCESS_KEY'])
        self.s3_client = session.client(service_name='s3',
                                        endpoint_url=kwargs['endpoint_url'])
        self.bucket = kwargs['copy_bucket']
        self.key = kwargs['copy_key']
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


    def verify_upload_end(self):
        self.__complete_multipart_upload()

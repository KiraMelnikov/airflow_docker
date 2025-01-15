from base64 import b64encode, b64decode
from tempfile import TemporaryDirectory
import subprocess
import json
import shutil
from retry import retry

class RolesAnywhereCredentials:

    def __init__(self, kwargs) -> None:
        self.byte_cert = kwargs.kwargs.get('byte_cert')
        self.byte_privateKey = kwargs.kwargs.get('byte_privateKey')
        self.trust_anchor_arn = kwargs.kwargs.get('trust_anchor_arn')
        self.profile_arn = kwargs.kwargs.get('profile_arn')
        self.role_arn = kwargs.kwargs.get('role_arn')
        self.temp_cert_name = kwargs.kwargs.get('temp_cert_name')
        self.temp_key_name = kwargs.kwargs.get('temp_key_name')
        self.temp_dir = TemporaryDirectory()
        self.path_cert = f"{self.temp_dir.name}/{self.temp_cert_name}"
        self.path_privat_key = f"{self.temp_dir.name}/{self.temp_key_name}"
        self.write_temp = self.__write_to_temp()
        self.show_temp_path = print(self.temp_dir.name)


    @staticmethod
    def __get_bytes_pem(path_name: str):
        with open(f'{path_name}', "rb") as f:
            return b64encode(f.read())

    def __write_to_temp(self):
        cert_out = b64decode(self.byte_cert)
        key_out = b64decode(self.byte_privateKey)
        with open(self.path_cert, "wb") as f:
            f.write(cert_out)
        with open(self.path_privat_key, "wb") as f:
            f.write(key_out)

    def remove_dir(self):
        shutil.rmtree(self.temp_dir.name)
        #os.removedirs(self.temp_dir.name)

    @retry(Exception, tries=3, delay=5, backoff=2)
    def get_credentials(self):
        result = subprocess.run(['aws_signing_helper', 'credential-process',
                               '--certificate', self.path_cert,
                               '--private-key', self.path_privat_key,
                               '--trust-anchor-arn', self.trust_anchor_arn,
                               '--profile-arn', self.profile_arn,
                               '--role-arn', self.role_arn],
                                  capture_output=True, text=True
                            )
        if len(result.stderr) > 0:
            raise RuntimeError(f'AWS signing helper returned error: {result.stderr}')

        creds = json.loads(result.stdout)
        return creds

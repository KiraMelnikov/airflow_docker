import os, sys
import tempfile
import pandas as pd
from retry import retry
sys.path.append("/opt/airflow/dags/sharepoint/ingestor")
from utils import get_kwarg_or_default
from datetime import datetime, date, timezone, timedelta

from office365.sharepoint.client_context import ClientContext
from office365.runtime.auth.user_credential import UserCredential
from office365.runtime.client_request_exception import ClientRequestException
from office365.runtime.auth.client_credential import ClientCredential


class SharepointDataLoader:

    def __init__(self, kwargs):

        self.site_url = "https://fozzy365.sharepoint.com/sites/"
        self.team = "DGOFileTransfer"
        self.chanel = kwargs["chanel"]
        self.folder_name = kwargs["folder_name"]
        self.file_name = kwargs["file_name"]
        self.path = "Shared Documents/{0}/{1}/{2}".format(self.chanel, self.folder_name, self.file_name)
        self.folder_path = os.path.dirname(self.path)
        self.credentials = UserCredential(kwargs["login"], kwargs["password"])
        self.column_names = kwargs["column_names"]
        self.column_types = kwargs["column_types"]
        self.parse_dates = get_kwarg_or_default("parse_dates", kwargs, None)
        self.ctx = ClientContext(self.site_url + self.team).with_credentials(self.credentials)
        self.web = self.ctx.web
        self.ctx.load(self.web)
        self._create_session()
        self._check_exist_folder()


    @retry(Exception, tries=3, delay=5, backoff=2)
    def _create_session(self) -> None:
        """Creating a connection to Sharepoint service"""
        try:
            self.ctx.execute_query()
        except Exception as err:
            raise RuntimeError(f"Failed to connect: {err}")


    @retry(Exception, tries=3, delay=2, backoff=2)
    def _check_exist_folder(self) -> None:
        """Checking source`s folder if exist"""
        try:
            self.web.get_folder_by_server_relative_url(self.folder_path).get().execute_query()
            print("Folder '{0}' is found".format(self.folder_path))
        except ClientRequestException as e:
            if e.response.status_code == 404:
                print("Folder '{0}' not found".format(self.folder_path))
            else:
                raise ValueError(e.response.text)


    @retry(Exception, tries=3, delay=2, backoff=2)
    def _download_file(self, download_path:str) -> None:
        """Download file from Sharepoint service to temp directory"""
        with open(download_path, "wb") as temp_file:
            (
                self.web.get_file_by_server_relative_path(self.path)
                .download(temp_file)
                .execute_query()
            )
            print(f"File has been downloaded at {download_path}")


    def _get_df(self, download_path:str) -> pd.DataFrame:
        """Convert binary file from temp dir to Pandas Dataframe"""
        if self.parse_dates:
            return pd.read_excel(download_path, header=0, names=self.column_names, dtype=self.column_types,
                                 parse_dates=[self.parse_dates])
        else:
            return pd.read_excel(download_path, header=0, names=self.column_names, dtype=self.column_types)


    def get_data(self) -> pd.DataFrame:
        """Getting Pandas Dataframe from temp dir"""
        with tempfile.TemporaryDirectory() as temp_dir:
            download_path = os.path.join(temp_dir, self.file_name)
            self._download_file(download_path)
            return self._get_df(download_path)

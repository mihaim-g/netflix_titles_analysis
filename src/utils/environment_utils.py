import os
from dotenv import load_dotenv


class EnvUtils:
    def __init__(self):
        load_dotenv(dotenv_path='.env')
        self._aws_profile = os.environ['AWS_PROFILE']
        self._dataset_file_name = os.environ['TITLES_FILE_PATH']
        self._dataframe_destination = os.environ['DATAFRAME_DESTINATION']
        self._dataframe_writing_mode = os.environ['OVERWRITE_FILE_MODE']
        if 'ENVIRONMENT' in os.environ:
            self._env = os.environ['ENVIRONMENT']
        else:
            self._env = None
        if 'S3_ENDPOINT' in os.environ:
            self._s3_endpoint = os.environ['S3_ENDPOINT']
        else:
            self._s3_endpoint = None

    @property
    def aws_profile(self) -> str:
        return self._aws_profile

    @property
    def dataset_file_name(self) -> str:
        return self._dataset_file_name

    @property
    def dataframe_destination(self) -> str:
        return self._dataframe_destination

    @property
    def dataframe_writing_mode(self) -> str:
        return self._dataframe_writing_mode

    @property
    def s3_endpoint(self) -> str:
        return self._s3_endpoint

    @property
    def env(self) -> str:
        return self._env

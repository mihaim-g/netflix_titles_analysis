
import boto3


class AWSUtils:
    def __init__(self, profile):
        self._aws_credentials = self._read_aws_credentials(profile=profile)

    def _read_aws_credentials(self, profile: str) -> tuple:
        session = boto3.Session(profile_name=profile)
        credentials = session.get_credentials()
        return(credentials.access_key, credentials.secret_key)

    def get_aws_credentials(self) -> tuple:
        return self._aws_credentials

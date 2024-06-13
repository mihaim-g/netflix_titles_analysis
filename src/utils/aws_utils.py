import boto3


class AWSUtils:
    def __init__(self, profile: str):
        self._aws_credentials = self._read_aws_credentials(profile=profile)

    @staticmethod
    def _read_aws_credentials(profile: str) -> tuple:
        session = boto3.Session(profile_name=profile)
        credentials = session.get_credentials()
        return credentials.access_key, credentials.secret_key

    def get_aws_credentials(self) -> tuple:
        return self._aws_credentials

from unittest.mock import patch, Mock
from src.pyspark.utils.aws_utils import AWSUtils

class TestAWSUtils:
    @patch('boto3.Session')
    def test_read_aws_credentials(self, mock_boto_session):
        mock_session = Mock()
        mock_credentials = Mock()
        mock_credentials.access_key = 'fake_access_key'
        mock_credentials.secret_key = 'fake_secret_key'
        mock_session.get_credentials.return_value = mock_credentials

        mock_boto_session.return_value = mock_session

        aws_utils = AWSUtils(profile='fake_profile')

        credentials = aws_utils.get_aws_credentials()

        assert credentials == ('fake_access_key', 'fake_secret_key')
        mock_boto_session.assert_called_with(profile_name='fake_profile')
        mock_session.get_credentials.assert_called_once()

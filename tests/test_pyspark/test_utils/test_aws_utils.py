from unittest.mock import patch, MagicMock, Mock
from src.pyspark.utils.aws_utils import AWSUtils

mock = Mock()




























# class TestAWSUtils:
    
#     @patch('boto3.Session')
#     def test_get_aws_credentials(self, mock_boto_session):
#         mock_session = MagicMock()
        
#         # Create a mock credentials object with access_key and secret_key attributes
#         mock_credentials = MagicMock()
#         mock_credentials.access_key = 'mock_access_key'
#         mock_credentials.secret_key = 'mock_secret_key'
        
#         # Set the mock session's get_credentials method to return the mock credentials
#         mock_session.get_credentials.return_value = mock_credentials
        
#         # Set the return value of the boto3.Session constructor to our mock session
#         mock_boto_session.return_value = mock_session
        
#         # Instantiate AWSUtils with a test profile
#         profile = 'test_profile'
#         aws_utils = AWSUtils(profile)
        
#         # Call get_aws_credentials and verify the output
#         credentials = aws_utils.get_aws_credentials()
#         assert credentials == ('mock_access_key', 'mock_secret_key')
        
#         # Verify that boto3.Session was called with the correct profile_name
#         mock_boto_session.assert_called_once_with(profile_name=profile)
#         # Verify that get_credentials was called once
#         mock_session.get_credentials.assert_called_once()
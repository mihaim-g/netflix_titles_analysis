from src.pyspark.read_dataset import get_aws_credentials


def test_get_aws_credentials(mocker):
    mock_session = mocker.Mock()
    mock_credentials = mocker.Mock()
    mock_credentials.access_key = 'fake_access_key'
    mock_credentials.secret_key = 'fake_secret_key'

    mock_session.get_credentials.return_value = mock_credentials

    mocker.patch('boto3.Session', return_value=mock_session)

    profile = 'test_profile'
    access_key, secret_key = get_aws_credentials(profile)

    assert access_key == 'fake_access_key'
    assert secret_key == 'fake_secret_key'
    
    

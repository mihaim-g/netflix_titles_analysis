from src.pyspark.read_dataset import get_aws_credentials, get_spark_session


def test_get_aws_credentials(mocker, aws_credentials):
    mock_session = mocker.Mock()
    mock_credentials = mocker.Mock()
    mock_credentials.access_key = aws_credentials[0]
    mock_credentials.secret_key = aws_credentials[1]

    mock_session.get_credentials.return_value = mock_credentials

    mocker.patch('boto3.Session', return_value=mock_session)

    profile = 'test_profile'
    access_key, secret_key = get_aws_credentials(profile)

    assert access_key == 'fake_access_key'
    assert secret_key == 'fake_secret_key'


def test_get_spark_session(test_spark_session):
    method_session = get_spark_session(('fake_access_key','fake_secret_key'))
    assert test_spark_session == method_session

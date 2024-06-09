from src.utils.spark_session import CreateSparkSession


class TestSparkUtils:
    def test_create_spark_session(self, spark_fixture):
        aws_creds = ('fake_access_key', 'fake_secret_key')
        spark_session = CreateSparkSession(aws_creds).get_spark_session()
        assert spark_fixture == spark_session

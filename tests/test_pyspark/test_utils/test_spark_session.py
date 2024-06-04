from unittest.mock import patch, MagicMock
from src.pyspark.utils.spark_session import CreateSparkSession

class TestSparkUtils:
    @patch('src.pyspark.utils.spark_session.SparkConf')
    @patch('src.pyspark.utils.spark_session.SparkSession')
    def test_create_spark_session(self, mock_spark_session, mock_spark_conf):
        mock_conf = MagicMock()
        mock_spark_conf.return_value = mock_conf

        mock_conf.set.return_value = mock_conf

        mock_spark = MagicMock()
        mock_spark_session.builder.config.return_value.getOrCreate.return_value = mock_spark

        aws_creds = ('fake_access_key', 'fake_secret_key')
        spark = CreateSparkSession(aws_creds).get_spark_session()

        assert spark == mock_spark
        mock_conf.set('fs.s3a.access.key', aws_creds[0])
        mock_conf.set('fs.s3a.secret.key', aws_creds[1])
        mock_spark_session.builder.config.assert_called_with(conf=mock_conf)
        mock_spark_session.builder.config.return_value.getOrCreate.assert_called_once()

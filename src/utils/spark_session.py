import pyspark
from pyspark import SparkConf
from pyspark.sql import SparkSession


class CreateSparkSession:
    def __init__(self, aws_credentials, env, s3_endpoint):
        self._spark_session = self._get_spark_session(aws_credentials, env, s3_endpoint)

    @staticmethod
    def _get_spark_session(aws_creds: tuple, env: str, s3_endpoint: str) -> pyspark.sql.session.SparkSession:
        conf = SparkConf()
        if env == 'DEV':
            conf.set("fs.s3a.endpoint", s3_endpoint)
        conf.set('fs.s3a.access.key', aws_creds[0])
        conf.set('fs.s3a.secret.key', aws_creds[1])
        return SparkSession.builder.config(conf=conf).getOrCreate()

    def get_spark_session(self):
        return self._spark_session

import pyspark
from pyspark import SparkConf
from pyspark.sql import SparkSession


class CreateSparkSession:
    def __init__(self, aws_credentials):
        self._spark_session = self._get_spark_session(aws_credentials)

    @staticmethod
    def _get_spark_session(self, aws_creds: tuple) -> pyspark.sql.session.SparkSession:
        conf = SparkConf()
        conf.set('fs.s3a.access.key', aws_creds[0])
        conf.set('fs.s3a.secret.key', aws_creds[1])
        return SparkSession.builder.config(conf=conf).getOrCreate()

    def get_spark_session(self):
        return self._spark_session

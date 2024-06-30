from pyspark import SparkConf
from pyspark.sql import SparkSession


class CreateSparkSession(object):
    _instance = None

    def __new__(cls, aws_credentials: tuple, env: str, s3_endpoint: str) -> None:
        if cls._instance is None:
            cls._instance = super(CreateSparkSession, cls).__new__(cls)
            cls._instance = cls._get_spark_session(aws_credentials, env, s3_endpoint)
        return cls._instance

    @staticmethod
    def _get_spark_session(aws_creds: tuple, env: str, s3_endpoint: str) -> SparkSession:
        conf = SparkConf()
        if env == 'DEV':
            conf.set("fs.s3a.endpoint", s3_endpoint)
            conf.set("fs.s3a.path.style.access", "true")
        conf.set('fs.s3a.access.key', aws_creds[0])
        conf.set('fs.s3a.secret.key', aws_creds[1])
        return SparkSession.builder.config(conf=conf).getOrCreate()

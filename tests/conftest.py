import pytest
import pyspark
from pyspark import SparkConf
from pyspark.sql import SparkSession


@pytest.fixture
def aws_credentials() -> tuple:
    return 'fake_access_key', 'fake_secret_key'


@pytest.fixture
def mocked_spark_session(aws_credentials):
    conf = SparkConf()
    conf.set('fs.s3a.access.key', aws_credentials[0])
    conf.set('fs.s3a.secret.key', aws_credentials[1])
    return SparkSession.builder.config(conf=conf).getOrCreate()
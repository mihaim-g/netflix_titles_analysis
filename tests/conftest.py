import pytest
from pyspark import SparkConf
from pyspark.sql import SparkSession


@pytest.fixture
def aws_credentials() -> tuple:
    return 'fake_access_key', 'fake_secret_key'


@pytest.fixture(scope="session")
def test_spark_session():
    conf = SparkConf()
    conf.set('fs.s3a.access.key', 'fake_access_key')
    conf.set('fs.s3a.secret.key', 'fake_secret_key')
    return SparkSession.builder.config(conf=conf).getOrCreate()

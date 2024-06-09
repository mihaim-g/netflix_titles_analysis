import pytest
from pyspark import SparkConf
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark_fixture():
    aws_creds = ('fake_access_key', 'fake_secret_key')
    conf = SparkConf()
    conf.set('fs.s3a.access.key', aws_creds[0])
    conf.set('fs.s3a.secret.key', aws_creds[1])
    spark = SparkSession.builder.appName("Testing PySpark Example").getOrCreate()
    yield spark

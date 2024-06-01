import os
import boto3
import pyspark
from pyspark import SparkConf
from pyspark.sql import SparkSession

aws_profile=os.environ['AWS_PROFILE']
FILE_NAME='s3a://mituca-repo1-raw-data/raw_input/netflix_titles.csv.gz'


def get_aws_credentials(profile: str) -> tuple:
    session = boto3.Session(profile_name=profile)
    credentials = session.get_credentials()
    return(credentials.access_key, credentials.secret_key)


def get_spark_session(aws_creds: tuple) -> pyspark.sql.session.SparkSession:
    conf = SparkConf()
    conf.set('fs.s3a.access.key', aws_creds[0])
    conf.set('fs.s3a.secret.key', aws_creds[1])
    return SparkSession.builder.config(conf=conf).getOrCreate()


if __name__ == '__main__':
    creds = get_aws_credentials(aws_profile)
    spark = get_spark_session(creds)
    print(type(spark))
    df = spark.read.csv(FILE_NAME, inferSchema=True)
    df.show(10)

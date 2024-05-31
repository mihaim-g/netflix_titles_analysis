import os
import boto3
from pyspark import SparkConf
from pyspark.sql import SparkSession

aws_profile='repo1-admin'

def get_aws_credentials(aws_profile: str) -> tuple:
    session = boto3.Session(profile_name=aws_profile)
    credentials = session.get_credentials()
    return(credentials.access_key, credentials.secret_key)

def get_spark_session(aws_creds: tuple):
    conf = SparkConf()
    conf.set('fs.s3a.access.key', aws_creds[0])
    conf.set('fs.s3a.secret.key', aws_creds[1])
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    return spark

if __name__ == '__main__':
    creds = get_aws_credentials(aws_profile)
    spark = get_spark_session(creds)
    df = spark.read.csv('s3a://mituca-repo1-raw-data/raw_input/netflix_titles.csv.gz', inferSchema=True)
    df.show(10)
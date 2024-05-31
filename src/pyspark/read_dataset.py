import os
import boto3
from pyspark.sql import SparkSession

aws_profile='repo1-admin'

def get_aws_credentials(aws_profile: str) -> tuple:
    session = boto3.Session(profile_name=aws_profile)
    credentials = session.get_credentials()
    return(credentials.access_key, credentials.secret_key)

def get_spark_session(aws_creds: tuple):
    spark = SparkSession.builder \
        .appName("Read Zipped CSV from S3") \
        .getOrCreate()
    return spark

if __name__ == '__main__':
    creds = get_aws_credentials(aws_profile)
    spark = get_spark_session(creds)
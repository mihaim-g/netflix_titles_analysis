import pyspark
from pyspark import SparkConf
from pyspark.sql import SparkSession

class CreateSparkSession:
    def __init__(self, aws_credetntials):
        self.__spark_session = self.__get_spark_session(aws_credetntials)

    def __get_spark_session(self, aws_creds: tuple) -> pyspark.sql.session.SparkSession:
        conf = SparkConf()
        conf.set('fs.s3a.access.key', aws_creds[0])
        conf.set('fs.s3a.secret.key', aws_creds[1])
        return SparkSession.builder.config(conf=conf).getOrCreate()
    
    
    def get_spark_session(self):
        return self.__spark_session


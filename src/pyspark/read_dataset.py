import os
from utils.aws_utils import AWSUtils
from utils.spark_session import CreateSparkSession

# TODO: move to a different module and class

aws_profile=os.environ['AWS_PROFILE']
FILE_NAME='s3a://mituca-repo1-raw-data/raw_input/netflix_titles.csv.gz'


if __name__ == '__main__':
    credentials = AWSUtils(aws_profile).get_aws_credentials()
    spark = CreateSparkSession(credentials).get_spark_session()
    df = spark.read.options(header=True, inferSchema=True).csv(FILE_NAME)
    
    # Drop columns that have only null vallues
    columns_to_drop = [col for col in df.columns if col.startswith('_c')]
    df = df.drop(*columns_to_drop)
    df.show(10)

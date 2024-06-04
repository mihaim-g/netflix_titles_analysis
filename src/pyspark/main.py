import os
from utils.aws_utils import AWSUtils
from utils.spark_session import CreateSparkSession
from dataframe_processing.netflix_titles import TitlesDF


aws_profile=os.environ['AWS_PROFILE']
FILE_NAME='s3a://mituca-repo1-raw-data/raw_input/netflix_titles.csv.gz'


if __name__ == '__main__':
    credentials = AWSUtils(aws_profile).get_aws_credentials()
    spark = CreateSparkSession(credentials).get_spark_session()
    titles_df = TitlesDF(spark, FILE_NAME).get_df()
    titles_df.show(10)

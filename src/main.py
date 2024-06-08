import os
from pyspark_files.utils.aws_utils import AWSUtils
from pyspark_files.utils.spark_session import CreateSparkSession
from pyspark_files.dataframe_processing.netflix_titles import Titles
from pyspark_files.dataframe_processing.users import Users


aws_profile=os.environ['AWS_PROFILE']
file_name=os.environ['TITLES_FILE_PATH']


if __name__ == '__main__':
    aws_utils = AWSUtils(aws_profile)
    credentials = aws_utils.get_aws_credentials()
    spark = CreateSparkSession(credentials).get_spark_session()

    titles = Titles(spark, file_name)
    # We'd like around 5 times as many users as titles, at least
    user_number = titles.get_df().count() * 5
    users = Users(spark, user_number)

    titles.get_df().show(10)
    users.get_df().show(10)

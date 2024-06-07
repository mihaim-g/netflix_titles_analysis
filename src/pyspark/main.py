import os
from utils.aws_utils import AWSUtils
from utils.spark_session import CreateSparkSession
from dataframe_processing.netflix_titles import Titles
from dataframe_processing.users import Users


aws_profile=os.environ['AWS_PROFILE']
file_name=os.environ['TITLES_FILE_PATH']


if __name__ == '__main__':
    credentials = AWSUtils(aws_profile).get_aws_credentials()
    spark = CreateSparkSession(credentials).get_spark_session()

    titles = Titles(spark, file_name)
    # We'd like around 5 times as many users as titles, at least
    user_number = titles.get_df().count() * 5
    users = Users(spark, user_number)

    titles.get_df().show(10)
    users.get_df().show(10)

import os
from utils.aws_utils import AWSUtils
from utils.spark_session import CreateSparkSession
from dataframe_processing.netflix_titles import Titles
from dataframe_processing.users import Users
from dataframe_processing.ratings import Ratings

aws_profile = os.environ['AWS_PROFILE']
dataset_file_name = os.environ['TITLES_FILE_PATH']
dataframe_destination = os.environ['DATAFRAME_DESTINATION']
dataframe_writing_mode = os.environ['OVERWRITE_FILE_MODE']


if __name__ == '__main__':
    aws_utils = AWSUtils(aws_profile)
    credentials = aws_utils.get_aws_credentials()
    spark = CreateSparkSession(credentials).get_spark_session()

    titles = Titles(spark, dataset_file_name)
    title_number = titles.titles_df.count()
    # We'd like around 5 times as many users as titles, at least
    user_number = title_number * 5
    users = Users(spark, user_number)
    ratings = Ratings(spark, user_number, title_number)

    titles.titles_df.show(10)
    users.users_df.show(10)
    ratings.ratings_df.show(10)

    # titles.titles_df.write.parquet(dataframe_destination + "titles/", mode=dataframe_writing_mode)
    # users.users_df.write.parquet(dataframe_destination + "users/", mode=dataframe_writing_mode)
    # ratings.ratings_df.write.parquet(dataframe_destination + "ratings/", mode=dataframe_writing_mode)

    # print(f"Max rating_id is: {ratings.get_df().agg({"id": "max"}).collect()[0][0]}")
    # print(f"Min rating_id is: {ratings.get_df().agg({"id": "min"}).collect()[0][0]}")
    # print(f"Max user_id is: {ratings.get_df().agg({"user_id": "max"}).collect()[0][0]}")
    # print(f"Min user_id is: {ratings.get_df().agg({"user_id": "min"}).collect()[0][0]}")
    # print(f"Max show_id is: {ratings.get_df().agg({"show_id": "max"}).collect()[0][0]}")
    # print(f"Min show_id is: {ratings.get_df().agg({"show_id": "min"}).collect()[0][0]}")]

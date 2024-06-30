from utils.environment_utils import EnvUtils
from utils.aws_utils import AWSUtils
from utils.spark_session import CreateSparkSession
from dataframe_processing.titles import Titles
from dataframe_processing.users import Users
from dataframe_processing.ratings import Ratings
from reccomendations.prepare_data import PrepareData


if __name__ == '__main__':
    env = EnvUtils()
    aws_utils = AWSUtils(env.aws_profile)
    credentials = aws_utils.get_aws_credentials()
    with CreateSparkSession(credentials, env.env, env.s3_endpoint) as spark:
        titles = Titles(spark, env.dataset_file_name)
        title_number = titles.titles_df.count()
        # We'd like around 5 times as many users as titles, at least
        user_number = title_number * 5
        users = Users(spark, user_number)
        ratings = Ratings(spark, user_number, title_number)

        # Saving dataframes to S3 for use in future enhancements
        # titles.save_parquet_to_s3(env.dataframe_destination, env.dataframe_writing_mode)
        # users.save_parquet_to_s3(env.dataframe_destination, env.dataframe_writing_mode)
        # ratings.save_parquet_to_s3(env.dataframe_destination, env.dataframe_writing_mode)
        
        prepared_data = PrepareData(ratings.ratings_df)
        prepared_data.prepared_data_df.show(10)

        # print(f"Max rating_id is: {ratings.get_df().agg({"id": "max"}).collect()[0][0]}")
        # print(f"Min rating_id is: {ratings.get_df().agg({"id": "min"}).collect()[0][0]}")
        # print(f"Max user_id is: {ratings.get_df().agg({"user_id": "max"}).collect()[0][0]}")
        # print(f"Min user_id is: {ratings.get_df().agg({"user_id": "min"}).collect()[0][0]}")
        # print(f"Max show_id is: {ratings.get_df().agg({"show_id": "max"}).collect()[0][0]}")
        # print(f"Min show_id is: {ratings.get_df().agg({"show_id": "min"}).collect()[0][0]}")]


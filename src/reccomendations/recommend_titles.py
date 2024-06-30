import sys

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, expr


class RecommendTitles:
    def __init__(self, ratings: DataFrame) -> None:
        self._recommendations_df = self._prepare_data(ratings)


    @property
    def recommendations_df(self) -> DataFrame:
        return self._recommendations_df

    @recommendations_df.setter
    def recommendations_df(self, df_value: DataFrame) -> None:
        self._recommendations_df = df_value

    @staticmethod
    def _prepare_data(df: DataFrame) -> DataFrame:
        df = RecommendTitles._most_popular_titles(df)
        df = RecommendTitles._filter_most_active_users(df, 10)
        return df

    @staticmethod
    def _most_popular_titles(df: DataFrame, percentage: float = 0.01) -> DataFrame:
        # This method gets top percentage% most popular  shows
        # If percentage = 0.05, we have top most 5% popular shows
        if 0.01 <= percentage <= 1:
            most_popular_count = int(df.count() * percentage)
            popularity_df = df.groupBy('show_id').agg(count('*').alias('popularity')) \
                .orderBy(col('popularity').desc()).limit(most_popular_count)
            return df.join(popularity_df, on='show_id', how='inner')
        else:
            print(f'{percentage} must be between 0.01 and 1')
            sys.exit(1)

    @staticmethod
    def _filter_most_active_users(df: DataFrame, minimum_rating_count: int = 5) -> DataFrame:
        if 5 <= minimum_rating_count:
            return df.withColumn("num_ratings", expr("count(*) over (partition by user_id)")) \
                .filter(col("num_ratings") >= minimum_rating_count)
        else:
            print(f'{minimum_rating_count} must be at least 5')
            sys.exit(1)



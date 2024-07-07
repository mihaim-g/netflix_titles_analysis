import sys
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, expr, rank
from pyspark.ml.feature import StringIndexer
from pyspark.sql.window import Window


class PrepareData:
    def __init__(self, ratings: DataFrame) -> None:
        self._prepared_data_df = self._prepare_data(ratings)

    @property
    def prepared_data_df(self) -> DataFrame:
        return self._prepared_data_df

    @prepared_data_df.setter
    def prepared_data_df(self, df_value: DataFrame) -> None:
        self._prepared_data_df = df_value

    @staticmethod
    def _prepare_data(df: DataFrame) -> DataFrame:
        df = PrepareData._most_popular_titles(df)
        df = PrepareData._filter_most_active_users(df, 10)
        df = PrepareData._mask_items(df)
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
            print(f'{percentage} for _most_popular_titles must be between 0.01 and 1')
            sys.exit(1)

    @staticmethod
    def _filter_most_active_users(df: DataFrame, minimum_rating_count: int = 5) -> DataFrame:
        if 5 <= minimum_rating_count:
            return (df.withColumn("num_ratings", expr("count(*) over (partition by user_id)")) \
                    .filter(col("num_ratings") >= minimum_rating_count))
        else:
            print(f'{minimum_rating_count} must be at least 5')
            sys.exit(1)

    @staticmethod
    def _mask_items(df: DataFrame, percentage: float = 0.25) -> DataFrame:
        if 0.01 <= percentage <= 1:
            df = df.withColumn("num_items_to_mask", (col("num_ratings") * percentage).cast("integer"))
            user_window = Window.partitionBy("user_id").orderBy(col("show_id").desc())
            df = df.withColumn("show_rank", rank().over(user_window))
            indexer_user = StringIndexer(inputCol="user_id", outputCol="user_index").setHandleInvalid("keep")
            indexer_item = StringIndexer(inputCol="show_id", outputCol="title_index").setHandleInvalid("keep")
            df = indexer_user.fit(df).transform(df)
            df = indexer_item.fit(df).transform(df)
            df = df.withColumn("user_index", df["user_index"].cast("integer")) \
                .withColumn("show_id", df["show_id"].cast("integer"))
            return df
        else:
            print(f'{percentage} for _mask_items must be between 0.01 and 1')
            sys.exit(1)

    def save_parquet_to_s3(self, destination: str, overwrite_mode: str) -> None:
        self.prepared_data_df.write.parquet(destination + 'prepared_data/', mode=overwrite_mode)

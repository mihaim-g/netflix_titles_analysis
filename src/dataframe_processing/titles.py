from pyspark.sql import DataFrame
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import substring_index


class Titles:
    def __init__(self, spark_session: SparkSession, file_name: str) -> None:
        self._titles_df = spark_session.read.options(header=True, inferSchema=True).csv(file_name)
        self.sanitize_input()

    @property
    def titles_df(self) -> DataFrame:
        return self._titles_df

    @titles_df.setter
    def titles_df(self, df_value: DataFrame) -> None:
        self._titles_df = df_value

    def sanitize_input(self) -> None:
        self.titles_df = self._drop_unwanted_columns(self.titles_df)
        self.titles_df = self._clean_show_id(self.titles_df)

    @staticmethod
    def _drop_unwanted_columns(df: DataFrame) -> DataFrame:
        columns_to_drop = [col for col in df.columns if col.startswith('_c')]
        return df.drop(*columns_to_drop)

    @staticmethod
    def _clean_show_id(df: DataFrame) -> None:
        return df.withColumn('show_id',substring_index(df["show_id"], "s", -1).alias("show_id"))

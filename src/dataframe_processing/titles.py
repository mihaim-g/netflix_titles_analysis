from pyspark.sql import DataFrame
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import substring_index, to_date
from pyspark.sql.types import IntegerType


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
        self.titles_df = self._format_date_added(self.titles_df)


    @staticmethod
    def _drop_unwanted_columns(df: DataFrame) -> DataFrame:
        columns_to_drop = [col for col in df.columns if col.startswith('_c')]
        return df.drop(*columns_to_drop)

    @staticmethod
    def _clean_show_id(df: DataFrame) -> DataFrame:
        df = df.withColumn('show_id', substring_index(df["show_id"], "s", -1))
        return df.withColumn('show_id', df['show_id'].cast(IntegerType()))

    @staticmethod
    def _format_date_added(df: DataFrame) -> DataFrame:
        return (df.withColumn('date_added', to_date(df['date_added'], 'MMMM dd, yyyy')).
                drop('release_year'))

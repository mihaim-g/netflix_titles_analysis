import logging
import random
from pyspark.sql import DataFrame
from pyspark.sql.session import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType

logger = logging.getLogger(__name__)


class Ratings:
    def __init__(self, spark_session: SparkSession, user_number: int, title_number: int) -> None:
        if isinstance(user_number, int) is True and 0 < user_number < 100000:
            self._ratings_df = self._generate_ratings_df(user_number, title_number, spark_session)
        else:
            logger.error("ERROR: Too many ratings, exiting!")
            self._ratings_df = None

    @staticmethod
    def _random_title_id(titles_number: int) -> str:
        return random.randrange(1, titles_number)

    @staticmethod
    def _random_user_id(user_number: int) -> int:
        return random.randrange(1, user_number+1)

    @staticmethod
    def _random_rating() -> int:
        return random.randrange(1, 6)

    def _generate_ratings_df(self, users: int, titles_number: int, spark: SparkSession) -> DataFrame:
        schema = StructType([
            StructField('id', IntegerType(), False),
            StructField('user_id', IntegerType(), False),
            StructField('show_id', IntegerType(), False),
            StructField('rating', IntegerType(), False),
        ])
        # An average of 10 ratings per user
        average_number_of_ratings_per_user = 10
        number_of_ratings = users * average_number_of_ratings_per_user
        ids = (i for i in range(1, number_of_ratings + 1))
        user_ids = (self._random_user_id(users) for i in range(1, number_of_ratings + 1))
        title_ids = (self._random_title_id(titles_number) for i in range(1, number_of_ratings + 1))
        ratings = (self._random_rating() for i in range(1, number_of_ratings + 1))
        return spark.createDataFrame(zip(ids, user_ids, title_ids, ratings), schema=schema)

    @property
    def ratings_df(self) -> DataFrame:
        return self._ratings_df

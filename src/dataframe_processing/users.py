import random
import logging
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import SparkSession, DataFrame
logger = logging.getLogger(__name__)


class Users:
    def __init__(self, spark_session: SparkSession, user_number: int) -> None:
        if isinstance(user_number, int) is True and 0 < user_number < 100000:
            self._users_df = self._generate_user_df(user_number, spark_session)
        else:
            logger.error("ERROR: Too many users, exiting!")
            self._users_df = None

    @property
    def users_df(self) -> DataFrame:
        return self._users_df

    @staticmethod
    def _generate_name() -> str:
        first_names = ('Mary', 'July', 'Andrea', 'Athena', 'Diana',
                       'Alexandra', 'John', 'Michael', 'Jeremiah', 'Bill')
        last_names = ('Dirk', 'Scott', 'Jensen', 'Miller', 'Killjoy',
                      'Jobs', 'Gates', 'Bezos', 'Musk', 'Titus')
        return random.choice(first_names)+" "+random.choice(last_names)

    def _generate_user_df(self, number: int, spark: SparkSession) -> DataFrame:
        schema = StructType([
            StructField('id', IntegerType(), False),
            StructField('name', StringType(), True)
        ])
        ids = (i for i in range(1, number+1))
        names = (self._generate_name() for i in range(0, number))
        return spark.createDataFrame(zip(ids, names), schema=schema)

    def save_parquet_to_s3(self, destination: str, overwrite_mode: str) -> None:
        self.users_df.write.parquet(destination + 'users/', mode=overwrite_mode)
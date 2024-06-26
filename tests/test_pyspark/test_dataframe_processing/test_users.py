from pyspark.sql.types import LongType,IntegerType ,StringType
from src.dataframe_processing.users import Users


class TestUsers:
    def test_init(self, spark_fixture):
        raw_input = zip([1, 2], ['a', 'b'])

        users = Users(spark_fixture, 2)
        actual_df = users.users_df
        expected_df = spark_fixture.createDataFrame(raw_input, ["id", "name"])

        assert isinstance(actual_df.schema["id"].dataType, IntegerType) is True
        assert isinstance(expected_df.schema["id"].dataType, LongType) is True
        assert isinstance(actual_df.schema["name"].dataType, StringType) is True
        assert isinstance(expected_df.schema["name"].dataType, StringType) is True
        assert actual_df.count() == expected_df.count()
        assert Users(spark_fixture, 100000).users_df is None
        assert Users(spark_fixture, 0).users_df is None
        assert Users(spark_fixture, '').users_df is None

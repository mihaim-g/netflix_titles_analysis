from src.dataframe_processing.ratings import Ratings

class TestRatings():
    def test_init(self, spark_fixture):
        ratings = Ratings(spark_fixture, 10, 5)
        assert ratings.get_df().count() == 100

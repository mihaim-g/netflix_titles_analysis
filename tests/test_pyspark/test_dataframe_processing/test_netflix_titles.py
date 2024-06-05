import os
from unittest.mock import patch
import pandas as pd
from src.pyspark.dataframe_processing.netflix_titles import TitlesDF

class TestTitlesDF:

    @patch.object(TitlesDF, 'sanitize_input')
    def test_init(self, mock_sanitize, spark_fixture):
        df = pd.DataFrame({
            'title': ['Title1', 'Title2'],
            'year': [2001, 2002]
        })
        temp_csv_file = 'test.csv'
        df.to_csv(temp_csv_file, index=False)

        instance = TitlesDF(spark_fixture, temp_csv_file)
        actual_df = instance.get_df()
        expected_df = spark_fixture.read.options(header=True, inferSchema=True).csv(temp_csv_file)

        assert actual_df.collect() == expected_df.collect()
        mock_sanitize.assert_called_once()
        os.remove(temp_csv_file)
        
    
    def test_drop_unwanted_columns(self, spark_fixture):
        
        

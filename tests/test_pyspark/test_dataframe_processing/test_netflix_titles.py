import os
from unittest.mock import patch
import pandas as pd
from src.pyspark.dataframe_processing.netflix_titles import TitlesDF

class TestTitlesDF:

    @patch.object(TitlesDF, 'sanitize_input')
    def test_init(self, mock_sanitize, spark_fixture):
        # Create a temporary CSV file
        df = pd.DataFrame({
            'title': ['Title1', 'Title2'],
            'year': [2001, 2002]
        })
        temp_csv_file = 'test.csv'
        df.to_csv(temp_csv_file, index=False)

    # Create an instance of MyClass with the Spark fixture and temporary CSV file
        instance = TitlesDF(spark_fixture, temp_csv_file)
    
    # Verify that the CSV file is read with the correct options
        actual_df = instance.get_df()
        expected_df = spark_fixture.read.options(header=True, inferSchema=True).csv(temp_csv_file)
    
        assert actual_df.collect() == expected_df.collect()
    
    # Verify that sanitize_input is called once
        mock_sanitize.assert_called_once()

    # Clean up the temporary CSV file
        os.remove(temp_csv_file)

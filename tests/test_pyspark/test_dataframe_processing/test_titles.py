import os
from unittest.mock import patch
import pandas as pd
from src.dataframe_processing.titles import Titles


class TestTitles:

    @patch.object(Titles, 'sanitize_input')
    def test_init(self, mock_sanitize, spark_fixture):
        df = pd.DataFrame({
            'title': ['Title1', 'Title2'],
            'year': [2001, 2002]
        })
        temp_csv_file = 'test.csv'
        df.to_csv(temp_csv_file, index=False)

        df_instance = Titles(spark_fixture, temp_csv_file)
        actual_df = df_instance.titles_df
        expected_df = spark_fixture.read.options(header=True, inferSchema=True).csv(temp_csv_file)

        assert actual_df.collect() == expected_df.collect()
        mock_sanitize.assert_called_once()
        os.remove(temp_csv_file)

    def test_sanitize_input(self, spark_fixture):
        df = pd.DataFrame(data={
            'show_id': ['s1', 's2'],
            'title': ['Title1', 'Title2'],
            'year': [2001, 2002],
            'date_added': ['21 September, 2001', '05 May, 2022']
        })
        temp_csv_file = 'test.csv'
        df.to_csv(temp_csv_file, index=False)
        clean_df = Titles(spark_fixture, temp_csv_file).titles_df
        df = pd.DataFrame(data={
            'show_id': ['s1', 's2'],
            'title': ['Title1', 'Title2'],
            'year': [2001, 2002],
            'date_added': ['21 September, 2001', '05 May, 2022'],
            '_c01': [None, None]
        })
        temp_csv_file = 'test.csv'
        df.to_csv(temp_csv_file, index=False)
        test_df = Titles(spark_fixture, temp_csv_file).titles_df
        assert test_df.collect() == clean_df.collect()
        os.remove(temp_csv_file)

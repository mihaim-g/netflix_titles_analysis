class Titles:
    def __init__(self, spark_session, file_name):
        self._titles_df = spark_session.read.options(header=True, inferSchema=True).csv(file_name)
        self.sanitize_input()

    @property
    def titles_df(self):
        return self._titles_df

    @titles_df.setter
    def titles_df(self, df_value):
        self._titles_df = df_value

    def sanitize_input(self):
        self.titles_df = self._drop_unwanted_columns(self.titles_df)

<<<<<<< HEAD
    @staticmethod
    def _drop_unwanted_columns(df):
<<<<<<< HEAD
=======
    @staticmehtod
    def _drop_unwanted_columns(self, df):
>>>>>>> 6944dfe (small changes)
=======
>>>>>>> 85fdf0f (Added setters and getters)
        columns_to_drop = [col for col in df.columns if col.startswith('_c')]
        return df.drop(*columns_to_drop)

class TitlesDF:
    def __init__(self, spark_session, file_name):
        raw_df = spark_session.read.options(header=True, inferSchema=True).csv(file_name)
        self.__titles_df = self.drop_unwanted_columns(raw_df)


    def get_df(self):
        return self.__titles_df

    def set_df(self, df):
        self.__titles_df = df

    def sanitize_input(self):
        self.set_df(self.drop_unwanted_columns(self.get_df()))

    def drop_unwanted_columns(self, df):
        columns_to_drop = [col for col in df.columns if col.startswith('_c')]
        return df.drop(*columns_to_drop)

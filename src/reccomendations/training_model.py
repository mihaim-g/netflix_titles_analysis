from pyspark.sql import DataFrame

class TrainModel:
    def __init__(self, prepared_data: DataFrame) -> None:
        self._prepared_data_df = prepared_data
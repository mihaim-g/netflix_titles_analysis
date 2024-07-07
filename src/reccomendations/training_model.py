import sys
import pandas
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, expr, collect_list
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator


class TrainModel:
    def __init__(self, prepared_data: DataFrame) -> None:
        self._user_eval = self._train_model(prepared_data)

    @property
    def user_eval(self) -> DataFrame:
        return self._user_eval

    @staticmethod
    def _train_model(df: DataFrame, rec_nr: int = 100) -> pandas.DataFrame:
        if not (1 <= rec_nr <= 1000):
            print(f'{rec_nr} for _train_model must be between 1 and 1000')
            sys.exit(1)
        train_df_rec = df.filter(col("show_rank") > col("num_items_to_mask"))
        test_df_rec = df.filter(col("show_rank") <= col("num_items_to_mask"))

        als = ALS(userCol='user_index', itemCol='title_index', ratingCol='rating',
                  coldStartStrategy='drop', nonnegative=True)

        param_grid = ParamGridBuilder() \
            .addGrid(als.rank, [1, 20, 30]) \
            .addGrid(als.maxIter, [20]) \
            .addGrid(als.regParam, [.05, .15]) \
            .build()
        evaluator = RegressionEvaluator(metricName='rmse', labelCol='rating', predictionCol='prediction')

        cv = CrossValidator(
            estimator=als,
            estimatorParamMaps=param_grid,
            evaluator=evaluator,
            numFolds=3)

        model = cv.fit(train_df_rec)

        best_model = model.bestModel
        model = als.fit(train_df_rec)

        predictions = best_model.transform(test_df_rec)
        predictions = predictions.withColumn("prediction", expr(
            "CASE WHEN prediction < 1 THEN 1 WHEN prediction > 5 THEN 5 ELSE prediction END"))

        evaluator = RegressionEvaluator(metricName='rmse', labelCol='rating', predictionCol='prediction')
        rmse = evaluator.evaluate(predictions)

        userRecs = best_model.recommendForAllUsers(rec_nr)

        user_ground_truth = (test_df_rec.groupby('user_index')
                             .agg(collect_list('title_index').alias('ground_truth_items')))
        user_train_items = (train_df_rec.groupby('user_index')
                            .agg(collect_list('title_index').alias('train_items')))

        user_eval = userRecs.join(user_ground_truth, on='user_index').join(user_train_items, on='user_index') \
            .select('user_index', 'recommendations.title_index', 'ground_truth_items', 'train_items',
                    'recommendations.rating')
        user_eval = user_eval.toPandas()
        user_eval['itemIndex_filtered'] = user_eval.apply(
            lambda x: [b for (b, z) in zip(x.title_index, x.rating) if b not in x.train_items], axis=1)
        user_eval['rating_filtered'] = user_eval.apply(
            lambda x: [z for (b, z) in zip(x.title_index, x.rating) if b not in x.train_items], axis=1)
        return user_eval

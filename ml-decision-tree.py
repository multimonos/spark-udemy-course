"""
Decision tree regressor

Question: can we predict the value of a house per unit area

- use a VectorAssembler to create the features vector
- features = VectorAssembler.setInputCols([f0, f1, f2,..., fn])
- the regressor is consumes a Tuple like (label, features)
"""
from typing import Tuple

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import DecisionTreeRegressor
from pyspark.sql import DataFrame, SparkSession, functions as fn


def load_data(ctx: SparkSession, path: str) -> DataFrame:
    return (
        ctx.read
        .option('header', True)
        .option('inferSchema', True)
        .csv(path)
    )


def prepare_data(df: DataFrame) -> DataFrame:
    """transform the structure of dataframe from just columns to something like (label:str, features:Vector)"""
    va = (
        VectorAssembler()
        .setInputCols(['HouseAge', 'DistanceToMRT', 'NumberConvenienceStores'])
        .setOutputCol('features')
    )

    return (
        va
        .transform(df)
        .withColumnRenamed('PriceOfUnitArea', 'label')  # what we are trying to predict
        .select('label', 'features')
    )


def split_data(df: DataFrame, train_weight: float, test_weight: float) -> Tuple[DataFrame, DataFrame]:
    train, test = df.randomSplit(df, [train_weight, test_weight])
    return train, test


def split_dataframe(df: DataFrame, training_weight: float, testing_weight: float) -> Tuple[DataFrame, DataFrame]:
    train, test = df.randomSplit([training_weight, testing_weight])
    return train, test


if __name__ == '__main__':
    spark = SparkSession.builder.getOrCreate()

    # load
    raw = load_data(spark, './data/realestate.csv')
    raw.show(5)

    # transform into required format for regression List[Tuple(label, features[]]
    df = prepare_data(raw)
    df.show(5)

    # split data into training / testing
    train_df, test_df = split_dataframe(df, .5, .5)
    train_df.show(5)
    test_df.show(5)

    # create a model
    dtr = (
        DecisionTreeRegressor()
        .setLabelCol('label')
        .setFeaturesCol('features')
    )
    trained_model = dtr.fit(train_df)

    # validate model
    predicted = trained_model.transform(test_df).cache()
    predicted.show()
    predicted.select('label', 'prediction').show()

    # add the squared error
    with_sqe = (
        predicted
        .withColumn('sqe', (predicted.label - predicted.prediction) ** 2)
    )
    with_sqe.show(5)

    # calculate mse
    mse = (
        with_sqe
        .agg(fn.avg('sqe').alias('mse'))
    )
    mse.show()

    # exit
    spark.stop()

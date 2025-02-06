"""
Linear regression is implemented via stochastic gradient descent

- sgd does not handle feature scaling well ... assumed data is normally distributed centred around zero
- need to have uniform scale
- assumes y-intercept is zero

Example,

Can we predict revenue based on page speed using linear model?
"""
from typing import List, Tuple

from pyspark.sql import SparkSession, DataFrame
from pyspark.rdd import RDD
from pyspark.ml.linalg import Vectors
from pyspark.ml.regression import LinearRegression


def load_dataset(ctx: SparkSession, path: str) -> DataFrame:
    rdd = ctx.sparkContext.textFile(path)

    ds = (
        rdd
        .map(lambda row: row.split(','))
        .map(lambda col: (float(col[0]), Vectors.dense(float(col[1]))))
    )

    colnames = ['label', 'features']

    df = ds.toDF(colnames)

    return df


def split_dataframe(df: DataFrame, training_weight: float, testing_weight: float) -> Tuple[DataFrame, DataFrame]:
    train, test = df.randomSplit([training_weight, testing_weight])
    return train, test


if __name__ == '__main__':
    spark = SparkSession.builder.getOrCreate()

    # load
    df = load_dataset(spark, './data/regression.txt')

    # split data into training / testing
    df_train, df_test = split_dataframe(df, .5, .5)

    # create a model
    lg = LinearRegression(maxIter=10, regParam=.3, elasticNetParam=.8)
    model = lg.fit(df_train)

    # validate model
    with_predictions = model.transform(df_test).cache()
    with_predictions.show()

    # weird ... why this method of extracting columns and not select 2 cols from df?
    predictions = with_predictions.select('prediction').rdd.map(lambda x: x[0])
    labels = with_predictions.select('label').rdd.map(lambda x: x[0])
    label_and_prediction = predictions.zip(labels).collect()

    for row in label_and_prediction:
        print(row)

    # exit
    spark.stop()

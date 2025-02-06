from typing import List, Tuple

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StringType, StructType, StructField, IntegerType
from pyspark.ml.recommendation import ALS
from pyspark.sql import functions as fn


def load_movie_names(ctx: SparkSession, schema: StructType, path: str) -> DataFrame:
    return (
        ctx.read
        .schema(schema)
        .option('sep', '|')
        .csv(path)
    )


def load_movie_ratings(ctx: SparkSession, schema: StructType, path: str) -> DataFrame:
    return (
        ctx.read
        .schema(schema)
        .option('sep', '\t')
        .csv(path)
    )


def movie_names_lookup(names: DataFrame, ids: List[str]) -> dict[int, str]:
    idstr = ','.join(map(str, ids))
    return (
        names
        .filter(fn.expr(f'movie_id IN ({idstr})'))
        .rdd
        .map(lambda row: (row.movie_id, row.name))
        .collectAsMap()
    )


if __name__ == '__main__':
    spark = SparkSession.builder.appName('ml-movie-reccommendations').getOrCreate()

    # movie names
    names_schema = StructType([
        StructField('movie_id', IntegerType(), True),
        StructField('name', StringType(), True),
    ])
    names = load_movie_names(spark, names_schema, './data/ml-100k/u.item')

    print('movie names:')
    names.show()

    # ratings
    ratings_schema = StructType([
        StructField('user_id', IntegerType(), True),
        StructField('movie_id', IntegerType(), True),
        StructField('rating', IntegerType(), True),
    ])
    movie_ratings = load_movie_ratings(spark, ratings_schema, './data/ml-100k/u.data')

    # display
    print(f'movie ratings:')
    movie_ratings.show()

    # add our test user
    test_user_id = 99999
    test_user_ratings = spark.createDataFrame([
        (test_user_id, 50, 5),
        (test_user_id, 172, 5),
        (test_user_id, 133, 1),
    ], schema=ratings_schema)
    test_ratings = movie_ratings.union(test_user_ratings)

    # display
    print('test_ratings:')
    test_ratings.filter(fn.expr(f'user_id == {test_user_id}')).show()

    # train model
    als = (
        ALS()
        .setMaxIter(5)
        .setRegParam(0.01)
        .setUserCol('user_id')
        .setItemCol('movie_id')
        .setRatingCol('rating')
    )
    model = als.fit(test_ratings)

    # add test user
    user_schema = StructType([
        StructField('user_id', IntegerType(), True)
    ])
    test_users = spark.createDataFrame([
        [test_user_id]
    ], schema=user_schema)

    # display
    print(f'test_users: count={test_users.count()}')
    test_users.show()

    # generate recommendations
    reccos = (
        model
        .recommendForUserSubset(test_users, 10)
    )

    # display
    print(f'recommendations: count={reccos.count()}')
    print(reccos)

    # names lookup
    rows = reccos.collect()
    movie_ids = [rec.movie_id for row in rows for rec in row.recommendations]
    names_lookup = movie_names_lookup(names, movie_ids)

    # display
    print(f'movie_ids: {movie_ids}')
    print(f'names_lookup: {names_lookup}')

    # final report
    # print(rows)

    print(f'recommendations by user:')
    for row in rows:
        user_id, recs = row
        for rec in recs:
            print(f'{user_id} : {rec.rating:.3f} : {names_lookup[rec.movie_id]} ')

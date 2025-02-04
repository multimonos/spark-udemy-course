import codecs

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as fn
from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType


def create_movie_rating_schema() -> StructType:
    return StructType([
        StructField('userid', IntegerType(), True),
        StructField('movieid', IntegerType(), True),
        StructField('rating', IntegerType(), True),
        StructField('timestamp', LongType(), True),
    ])


def create_movie_title_udf(con: SparkSession, movies: dict):
    movies_br = con.sparkContext.broadcast(movies)

    def get_movie_name(movieid: int) -> str:
        names = movies_br.value
        return names.get(movieid, "Unknown movie")

    return fn.udf(get_movie_name, StringType())


def load_movies(path: str) -> dict[int, str]:
    """
    Using codecs here as the file is not utf-8 encoded.

    Easiest way to check file encoding is `file -I /path/to/file`
    """
    names = {}
    with codecs.open(path, 'r', encoding='ISO-8859-1', errors='ignore') as f:
        for line in f:
            fields = line.split('|')
            names[int(fields[0])] = fields[1]
    return names


def load_ratings(con: SparkSession, schema: StructType, path: str) -> DataFrame:
    return (
        con.read
        .schema(schema)
        .option('sep', '\t')
        .csv(path)
    )


def most_popular_movies(df: DataFrame, title_udf) -> DataFrame:
    """this doesn't really calculate most popular movie just most frequently watched"""
    return (
        df
        .groupby(ratings.movieid)
        .count()
        .orderBy(fn.desc('count'))
        .withColumn('title', title_udf(df['movieid']))
    )


if __name__ == '__main__':
    spark = SparkSession.builder.getOrCreate()

    # movie names lookup
    movies = load_movies('./data/ml-100k/u.item')
    print(f'names.count: {len(movies)}')

    # create udf for movie title
    movie_title_udf = create_movie_title_udf(spark, movies)

    # movie ratings
    schema = create_movie_rating_schema()
    ratings = load_ratings(spark, schema, './data/ml-100k/u.data')
    print(f'ratings.count: {ratings.count()}')

    # final report
    report = most_popular_movies(ratings, movie_title_udf)
    report.select('title', 'count').show(10, truncate=False)

    spark.stop()

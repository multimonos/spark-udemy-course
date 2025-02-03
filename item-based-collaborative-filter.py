from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, IntegerType, LongType, StringType
from pyspark.sql import functions as fn


def load_movie_ratings(ctx: SparkSession, schema: StructType, path: str) -> DataFrame:
    return (
        ctx.read
        .schema(schema)
        .option('sep', '\t')
        .csv(path)
        .select('user_id', 'movie_id', 'rating')
    )


def load_movie_names(ctx: SparkSession, schema: StructType, path: str) -> DataFrame:
    return (
        ctx.read
        .schema(schema)
        .option('sep', '|')
        .option('charset', 'ISO-8859-1')
        .csv(path)
        .select('movie_id', 'name')
    )


def generate_movie_pairs(ratings: DataFrame) -> DataFrame:
    """
    self join to create list of movie pairs

    clause1
    - the self join "r0.user_id == r1.user_id"

    clause2
    - elimiates duplicate pairs as (a,b)==(b,a) ... just transposed
    - each pair has 2 unique movie_id tuples like { (3,4), (4,3) }
    - we can eliminate 1 of the sets if we require that movie0.id < movie1.id bc ids are increasing and unique
    """
    return (
        ratings.alias('r0')
        .join(ratings.alias('r1'), fn.expr('r0.user_id == r1.user_id AND r0.movie_id < r1.movie_id'))
        .select(
            fn.col('r0.movie_id').alias('movie0'),
            fn.col('r1.movie_id').alias('movie1'),
            fn.col('r0.rating').alias('rating0'),
            fn.col('r1.rating').alias('rating1'),
        )
    )


def compute_cosine_similarity(movie_rating_pairs: DataFrame) -> DataFrame:
    with_similarity_data = (
        movie_rating_pairs
        .withColumn('xx', fn.col('rating0') * fn.col('rating0'))  # a*a
        .withColumn('yy', fn.col('rating1') * fn.col('rating1'))  # b*b
        .withColumn('xy', fn.col('rating0') * fn.col('rating1'))  # a*b
    )
    pairs = (
        with_similarity_data
        .groupby('movie0', 'movie1')
        .agg(
            fn.sum(fn.col('xy')).alias('numerator'),
            (fn.sqrt(fn.sum(fn.col('xx'))) * fn.sqrt(fn.sum(fn.col('yy')))).alias('denominator'),
            fn.count(fn.col('xy')).alias('view_count')
        )
    )

    rs = (
        pairs
        .withColumn('score',
                    fn.when(fn.expr('denominator != 0'), fn.col('numerator') / fn.col('denominator')).otherwise(0))
        .select('movie0', 'movie1', 'score', 'view_count')
    )

    return rs


def movie_name(movie_names: DataFrame, movie_id: int) -> str:
    movie = (
        movie_names
        .select('name')
        .filter(fn.expr(f'movie_id == {movie_id}'))
        .first()
    )
    return movie.name


"""
Item based collaborative filtering with dataframe caching

Logic,
- for every movie in the db create pairs of movies
- for each movie pair for a user calc the similarity


"""
if __name__ == '__main__':
    spark = SparkSession.builder.getOrCreate()

    # schema
    movie_rating_schema = StructType([
        StructField('user_id', IntegerType(), True),
        StructField('movie_id', IntegerType(), True),
        StructField('rating', IntegerType(), True),
        StructField('timestamp', LongType(), True),
    ])

    movie_names_schema = StructType([
        StructField('movie_id', IntegerType(), True),
        StructField('name', StringType(), True),
    ])

    # movie ratings load
    ratings = load_movie_ratings(spark, movie_rating_schema, './data/ml-100k/u.data')
    print(f'ratings.count: {ratings.count()}')
    ratings.show(5)

    # movie names load
    movie_names = load_movie_names(spark, movie_names_schema, './data/ml-100k/u.item')
    print(f'movie_names.count: {movie_names.count()}')
    movie_names.show(5)

    # foreach user computer their movie pairs
    rated_pairs = generate_movie_pairs(ratings)
    print(f'pairs.count: {rated_pairs.count()}')
    rated_pairs.show(5)

    # compute the similarity and CACHE!!!
    similarity = compute_cosine_similarity(rated_pairs).cache()
    print('similarity:')
    similarity.show(5)

    # calculate a recommendation for a movie
    movie_id = 50
    min_view_count = 50
    threshold = .97

    filtered = (
        similarity
        .filter(
            (fn.expr(f'movie0 ={movie_id} OR movie1 = {movie_id}'))
            & (fn.expr(f'score >= {threshold}'))
            & (fn.expr(f'view_count >= {min_view_count}'))
        )
    )

    results = (
        filtered
        .orderBy(fn.col('score').desc())
        .take(10)  # -> List
    )
    print(f'results: count={len(results)}')

    # only print out the "other movie"
    for rs in results:
        mid = rs.movie1 if rs.movie0 == movie_id else rs.movie0
        name = movie_name(movie_names, mid)
        print(f'score={rs.score:.4f}  view_count={str(rs.view_count).rjust(5)}  {name}')

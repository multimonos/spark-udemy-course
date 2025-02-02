"""
find the most popular super hero

Required
- find the hero that has appeared with the most other heros

Given
- 2 datafiles
- marvel-graph.txt
    - which heros have appeared together in comic books
    - schema
        - whitespace delimited list of integers
        - hero_id : first entry per line, may appear as first entry on multiple lines
        - other_hero_ids : one or more integers, space demlim

- marvel-names.txt
    - names of the heros
    - schema
        - hero_id
        - hero_name

Strategy
- for each line in marvel-graph create ( hero_id, count(other_hero_ids) )
- reducebykey
- create a hero_name_lookup dict
- use to display the hero with highest other_hero_ids_count
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as fn
from pyspark.sql.types import IntegerType, StringType, StructField, StructType


def loadHeroGraph(con: SparkSession, path: str) -> DataFrame:
    return (
        con.read.text(path)
        .withColumn('hero_id', fn.split(fn.col('value'), ' ')[0])
        .withColumn('connected_hero_ids', fn.split(fn.col('value'), ' ', 2)[1])
        .drop('value')
        # alternate version of above using fns only
        # .withColumn('connected_hero_count', fn.size(fn.split(fn.col('value'), ' ')).cast('int') - fn.lit(1))
    )


def createHeroNameSchema() -> StructType:
    return StructType([
        StructField('hero_id', IntegerType(), True),
        StructField('name', StringType(), True),
    ])


def loadHeroNames(con: SparkSession, schema: StructType, path: str) -> DataFrame:
    return (
        con.read
        .schema(schema)
        .option('sep', ' ')
        .csv(path)
    )


def heroesWithName(names: DataFrame, heroes: DataFrame) -> DataFrame:
    return (
        heroes
        .join(names, on='hero_id', how='inner')
        .select('name', 'conn_count')
        .orderBy('name')
    )


def heroesByConnectionCount(heroes: DataFrame, count: int) -> DataFrame:
    return heroes.filter(fn.col('conn_count') == fn.lit(count))


def minHeroConnectionCount(heroes: DataFrame) -> int:
    row = heroes.orderBy(fn.col('conn_count').asc()).first()
    return int(row.conn_count)


def maxHeroConnectionCount(heroes: DataFrame) -> int:
    row = heroes.orderBy(fn.col('conn_count').desc()).first()
    return int(row.conn_count)


def tallyHeroConnections(df: DataFrame) -> DataFrame:
    with_counts = df.withColumn('conn_count', fn.expr("size(split(connected_hero_ids,' ')) - 1"))
    return (
        with_counts
        .groupby('hero_id')
        .agg(fn.sum('conn_count').alias('conn_count'))
    )


if __name__ == '__main__':
    spark = SparkSession.builder.getOrCreate()

    # hero names
    hero_name_schema = createHeroNameSchema()
    hero_names = loadHeroNames(spark, hero_name_schema, './data/marvel-names.txt')
    hero_names.show()

    # load the hero connection graph
    hero_graph = loadHeroGraph(spark, './data/marvel-graph.txt')
    print(f'raw.graph.count: {hero_graph.count()}')
    hero_graph.show()

    # tally the hero connections
    hero_conns = tallyHeroConnections(hero_graph)
    hero_conns.show()

    # most popular
    max_count = maxHeroConnectionCount(hero_conns)
    popular_heroes = heroesByConnectionCount(hero_conns, max_count)
    print(f'max: {max_count}, count={popular_heroes.count()}')
    heroesWithName(hero_names, popular_heroes).show(truncate=False)

    # least popular
    min_count = minHeroConnectionCount(hero_conns)
    unpopular_heroes = heroesByConnectionCount(hero_conns, min_count)
    print(f'min: {min_count}, count={unpopular_heroes.count()}')
    heroesWithName(hero_names, unpopular_heroes).show(truncate=False)

    # one connection)
    single_relation_heroes = heroesByConnectionCount(hero_conns, 1)
    print(f'with a single connection only, count={single_relation_heroes.count()}')
    heroesWithName(hero_names, single_relation_heroes).show()

    # exit
    spark.stop()

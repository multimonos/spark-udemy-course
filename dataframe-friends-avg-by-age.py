from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, round


def showAverageFriendsByAgeUsingSql(df: DataFrame) -> None:
    """according to shatgippity this is the least performant way to accomplish the task"""
    cdf = df.cache()
    cdf.createOrReplaceTempView('users')  # not performant

    # harder to debug as fns are not generating errors
    # optimization does not happen auto-magically
    rs = spark.sql("""
        select 
            age, 
            count(*) as cnt, 
            sum(friends) as total, 
            cast((total / cnt) as decimal(10,2)) as avg 
        from users 
        group by age 
        order by avg desc
        limit 5
    """)

    rs.select('age', 'avg').show()


def showAverageFriendsByAgeUsingOrm(df: DataFrame) -> None:
    (
        df
        .select('age', 'friends')
        .groupby('age')
        .agg(
            round(avg(df.friends), 2).alias('avg')
        )
        .orderBy('avg', ascending=False)
        .limit(5)
        .show()
    )


if __name__ == '__main__':
    spark = SparkSession.builder.getOrCreate()

    dataframe = (
        spark.read
        .option('header', 'true')
        .option('inferSchema', 'true')
        .csv('./data/fakefriends-header.csv')
    )

    dataframe.printSchema()

    showAverageFriendsByAgeUsingSql(dataframe)

    showAverageFriendsByAgeUsingOrm(dataframe)

    spark.stop()

from pyspark.sql import SparkSession
from pyspark.sql import Row


def createPerson(row: str):  # str -> Row
    r = row.split(',')

    return Row(
        id=int(r[0]),
        name=str(r[1]).encode('utf-8'),
        age=int(r[2]),
        friendCount=int(r[3]),
    )


if __name__ == '__main__':
    # create session
    spark = SparkSession.builder.getOrCreate()

    # load
    lines = spark.sparkContext.textFile('./data/fakefriends.csv')  # path:string -> RDD
    print(f'count: {lines.count()}')

    # schema
    people = lines.map(createPerson)

    # dataframe
    df = spark.createDataFrame(people).cache()

    # create a sql alias/referent ='peope' for use in spark.sql(...)
    df.createOrReplaceTempView('people')

    # query : teenagers
    teenagers = spark.sql('SELECT * FROM people WHERE age >=13 AND age <= 19')

    # display : loop
    print(f'teenagers.count: {teenagers.count()}')
    for person in teenagers.collect():
        print(person)

    # display : using spark fns
    count_df = df.groupBy('age').count()
    count_df.orderBy('age').show()
    count_df.orderBy(df.age.desc()).show()

    # alernate chaining syntax
    (
        count_df
        .filter((df.age >= 13) & (df.age <= 19))
        .orderBy('age')
        .show()
    )

    # done
    spark.stop()

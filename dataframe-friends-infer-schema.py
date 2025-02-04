from pyspark.sql import SparkSession

if __name__ == '__main__':
    # create session
    spark = SparkSession.builder.getOrCreate()

    # load
    people = (
        spark.read
        .option('header', 'true')
        .option('inferSchema', 'true')
        .csv('./data/fakefriends-header.csv')
    )

    # display inferred schema
    print('Person:')
    people.printSchema()

    print('name column:')
    people.select('name').show()

    print('only under 21:')
    people.filter(people.age <= 21).show()

    print('group by age:')
    people.groupby(people.age).count().show()

    print('inc age by 10 years:')
    people.select(people.name, people.age, people.age + 10).show()

    # done
    spark.stop()

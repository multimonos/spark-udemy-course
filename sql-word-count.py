from pyspark.sql import SparkSession
from pyspark.sql import functions as fn

if __name__ == '__main__':
    spark = SparkSession.builder.getOrCreate()

    # load
    df = spark.read.text('./data/book.txt')
    print(f'lines: {df.count()}')

    strings = (
        df
        .select(
            fn.explode(fn.split(df.value, "\\W+")).alias('word')
        )
    )
    print(f'strings.count: {strings.count()}')

    words = strings.filter(strings.word != '')
    print(f'words.count: {words.count()}')

    lowercase_words = words.select(fn.lower(words.word).alias('word'))

    wordcounts = lowercase_words.groupby('word').count()

    wordcounts.orderBy('count', ascending=False).show()

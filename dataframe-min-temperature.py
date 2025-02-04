from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as fn
from pyspark.sql.types import FloatType, IntegerType, StringType, StructField, StructType


def createTemperatureSchema() -> StructType:
    return StructType([
        StructField('uuid', StringType(), True),
        StructField('date', IntegerType(), True),
        StructField('sampleType', StringType(), True),
        StructField('temperature', FloatType(), True),  # 10th of degree celsius
    ])


def loadTemperatureData(ctx: SparkSession, schema: StructType, path: str) -> DataFrame:
    df = (
        ctx.read
        .schema(schema)
        .csv(path)
    )
    return df


def minimumTemperatureByStation(df: DataFrame) -> DataFrame:
    # keep min temp readings only
    filtered = df.filter(df.sampleType == 'TMIN')

    # keep min temp value for each station
    mins = (
        filtered
        .select('uuid', 'temperature')
        .groupby('uuid')
        .agg(fn.min('temperature').alias('min_temp'))
    )

    # Add report columns

    # Note that below fn.expr() and fn.col() are equivalent and equally performant, however, the ide, will complain
    # about usage of fn.col() because the `* .1` operation is implied to be a column wise op.
    # Using fn.expr() we can make the columnwise more explicit.
    rs = (
        mins
        .withColumn('min_f', fn.round(fn.expr('min_temp * .1 * (9 / 5) + 32'), 2))  # expr() -> Column
        .withColumn('min_c', fn.round(fn.col('min_temp') * .1, 2))  # same performance, but, ide reports "type" issue
    )

    return rs


if __name__ == '__main__':
    spark = SparkSession.builder.getOrCreate()

    schema = createTemperatureSchema()
    print(f'schema:{schema}')  # debug

    df = loadTemperatureData(spark, schema, './data/1800.csv')

    df.printSchema()
    print(f'raw.count: {df.count()}')  # debug

    report = minimumTemperatureByStation(df)
    print(f'report.count: {report.count()}')  # debug

    report.orderBy(report.min_c, ascending=False).show()

    spark.stop()

from typing import Final

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as fn
from pyspark.sql.types import FloatType, IntegerType, StructField, StructType

TOTAL: Final = 'total_cost'


def createOrderSchema() -> StructType:
    return StructType([
        StructField('customerid', IntegerType(), True),
        StructField('productid', IntegerType(), True),
        StructField('cost', FloatType(), True),
    ])


def loadOrders(ctx: SparkSession, schema: StructType, path: str) -> DataFrame:
    return ctx.read.schema(schema).csv(path)


def amountSpentByCustomer(df: DataFrame) -> DataFrame:
    return (
        df
        .groupby(df.customerid)
        .agg(fn.sum(df.cost).alias(TOTAL))
        .withColumn(TOTAL, fn.round(TOTAL, 2))
        .orderBy(TOTAL, ascending=False)
    )


if __name__ == '__main__':
    spark = SparkSession.builder.getOrCreate()

    schema = createOrderSchema()

    orders = loadOrders(spark, schema, './data/customer-orders.csv')
    print(f'count: {orders.count()}')

    report = amountSpentByCustomer(orders)
    report.show()

    spark.stop()

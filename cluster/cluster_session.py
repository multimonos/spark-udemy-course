from pyspark.sql import SparkSession


def local_spark_cluster_session() -> SparkSession:
    return (
        SparkSession.builder
        .master('spark://spark-master:7077')  # match with docker config
        # .master('spark://spark-master:7077')  # match with docker config
        .config("spark.driver.memory", "4g")  # match with docker limits
        .config("spark.driver.maxResultSize", "2g")
        .config('spark.cores.max', 6)  # match with docker config
        .config("spark.executor.cores", 3)  # match with docker config
        .config('spark.executor.memory', '4g')  # match with docker config
        # parallelism + shuffle
        .config("spark.default.parallelism", 6)  # match core usage, 1 per core
        .config("spark.sql.shuffle.partitions", 6)
        # memory management
        .config("spark.memory.fraction", 0.8)  # heap space for execution and storage
        .config("spark.memory.storageFraction", 0.3)  # storage of cached data
        # garbage collection
        .config("spark.executor.extraJavaOptions", "-XX:+UseG1GC")  # arm optimized
        .config("spark.driver.extraJavaOptions", "-XX:+UseG1GC")  # arm optimized
        .getOrCreate()
    )


def test_local_spark_cluster_sesssion() -> SparkSession:
    spark = (
        SparkSession.builder
        .master('spark://spark-master:7077')
        .getOrCreate()
    )

    print(f'spark.version: {spark.version}')

    df = spark.range(5)
    df.show()

    spark.stop()

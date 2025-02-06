from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, functions as fn


def parse_logs(df: DataFrame) -> DataFrame:
    r_content_size = r'\s(\d+)$'
    r_general = ''
    r_timestamp = r''
    r_status = r'\s(\d{3})\s'
    r_host = r'^([\d\.]+)\s+\-'

    return df.select(
        fn.regexp_extract('value', r_host, 1).alias('host'),
        fn.regexp_extract('value', r_status, 1).alias('status')
    )


if __name__ == '__main__':
    # log path
    log_path = Path('./logs')

    if not log_path.is_dir():
        raise FileNotFoundError(f'Directory not found {log_path}')

    # create a context
    spark = (
        SparkSession
        .builder
        .appName('structured_streaming')
        .getOrCreate()  # supports resume
    )
    spark.sparkContext.setLogLevel('ERROR')

    # watch directory for new data files
    # data = spark.read.text(str(log_path))  # -> DataFrame
    data = spark.readStream.text(str(log_path))  # -> DataFrame

    # transfrom
    log_df = parse_logs(data)

    # status counts
    status_counts = log_df.groupby('status').count()

    # start watching
    query = (
        status_counts
        .writeStream
        .outputMode('complete')
        .format('console')
        .queryName('logs')
        .start()
    )

    # watch
    query.awaitTermination()

    # shutdown spark
    spark.stop()

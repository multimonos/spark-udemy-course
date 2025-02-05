from cluster.cluster_session import test_local_spark_cluster_sesssion

if __name__ == '__main__':

    test_local_spark_cluster_sesssion()

    # spark = local_spark_cluster_session()
    #
    #
    # df = spark.range(100).toDF('number')
    #
    # df.show()
    #
    # spark.stop()

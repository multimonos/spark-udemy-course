from pyspark import SparkConf, SparkContext

# configure spark
conf = SparkConf().setMaster('local').setAppName('rdd_map_app')

# create spark context
sc = SparkContext(conf=conf)

rdd = sc.parallelize([1, 2, 3, 4, 5])

# square each number with lambda
squared = rdd.map(lambda x: x * x)
print(squared.collect())
squared.foreach(lambda x: print(x))


# cube each term with fn
def cubed(x):
    return x * x * x


# note that map was not destructive
cubed = rdd.map(cubed)
cubed.foreach(lambda x: print(x))


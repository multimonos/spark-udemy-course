from pyspark import SparkConf, SparkContext


def cubed(x):
    return x * x * x


# configure spark
conf = SparkConf().setMaster('local').setAppName('rdd_action_examples')

# create spark context
sc = SparkContext(conf=conf)

rdd = sc.parallelize([1, 2, 3, 4, 5])

cubed = rdd.map(cubed)

print('cubed:')
cubed.foreach(lambda x: print(x))

print('count:')
print(cubed.count())

print('countByValue:')
histogram = cubed.countByValue()
for key, val in histogram.items():
    print(f'{key}: {val}')

print('top:')
top3 = cubed.top(3)
print(top3)

print('reduce:')
running_tally = cubed.reduce(lambda a, b: a + b)
print(running_tally)

# reduce by key
rdd = sc.parallelize(["apple", "banana", "apple", "orange", "banana", "apple"])
pairs = rdd.map(lambda word: (word, 1))
counts = pairs.reduceByKey(lambda a, b: a + b)
print(counts.collect())

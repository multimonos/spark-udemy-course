"""
Notes

A few ways to run this script,
- python ./ratings-counter.py
- spark-submit ./ratings-counter.py

Analyzes a bunch of movie data to generate a histogram of each rating number.
"""
from pyspark import SparkConf, SparkContext
import collections

"""
set up spark
.setMaster('local') ... 1 local node only
.setAppName('appname') ... look up the appname in spark ui
"""
conf = SparkConf().setMaster('local').setAppName('ratings_histogram')
sc = SparkContext(conf=conf)  # create spark context

"""
ingest movie data
.textFile() -> each line of text in file represented by a single entry in return value
"""
lines = sc.textFile('./data/ml-100k/u.data')

# rdd is not transformed in place
ratings = lines.map(lambda x: x.split()[2])  # extract the 2nd value aka "rating value"

# count the movie ratings instances
result = ratings.countByValue()

# sort the data
sortedResults = collections.OrderedDict(sorted(result.items()))

# display
for key, value in sortedResults.items():
    print(f'{key} {value:,}')

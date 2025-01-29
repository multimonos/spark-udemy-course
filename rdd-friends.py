from pyspark import SparkConf, SparkContext

"""
Note 
The `reduceByKey` fn does NOT operate on a dictionary.  It does, however, operate on a tuple where the first entry
in the tuple is assumed to be the key.  This allows for non-unique keys in a list of tuples.

So, we could rename the function `reduceByKey` to `reduceByFirstEntryInTuple` in an effort to more aptly capture the
behaviour / implicit assumptions in the fn.
"""


def parseLine(line):
    fields = line.split(',')
    age = int(fields[2])
    friend_count = int(fields[3])
    return age, friend_count


def createTuple(friend_count):
    return friend_count, 1  # friendcount,ageOccurrences


def sumFriendCountAndAgeOccurences(acc, cur):
    total_friends = acc[0] + cur[0]
    age_occurrences = acc[1] + cur[1]
    return total_friends, age_occurrences


# configure spark
conf = SparkConf().setMaster('local').setAppName('friends_by_age')

# create spark context
sc = SparkContext(conf=conf)

lines = sc.textFile('./data/fakefriends.csv')

rdd = lines.map(parseLine)
print(rdd.take(5))

"""
transform values
(int, int)[] -> (int, (int, int)[]
(age, friendcount)[] -> (age, (friend_count, age_occurrences))[]
"""
vals = rdd.mapValues(createTuple)
print(vals.take(5))

"""
count number of friends and occurences of a age
( int, (int,int) )[] -> (int, (int,int) )[]
( age, (count,ageOccurrences) )[] -> (age, (totalFriends, ageOccurrences) )[]
"""
totalsByAge = vals.reduceByKey(sumFriendCountAndAgeOccurences)
print(totalsByAge.collect())

# average friends by age
averageFriendsByAge = totalsByAge.mapValues(lambda v: v[0] / v[1])

# result
rs = averageFriendsByAge.collect()

for x in rs:
    print(x)

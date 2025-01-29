from pyspark import SparkConf, SparkContext


def entryTypeIsMin(x):
    """keep only a minimum temperature entry"""
    return 'TMIN' in x


def toCelcius(t):
    c = float(t) * .1 * (9. / 5.) + 32.
    return c


def parseLine(line):
    vec = line.split(',')
    uuid = vec[0]
    stype = vec[2]
    temperature = toCelcius(vec[3])
    data = (uuid, stype, temperature)
    return data


def keepMinOf(x, y):
    """only keep minimum of x and y values"""
    return min(x, y)


# configure
conf = SparkConf().setMaster('local').setAppName('friends_by_age')
sc = SparkContext(conf=conf)

# load
raw = sc.textFile('./data/1800.csv')

# load
print(raw.count())
entries = raw.map(parseLine)
print(f'entries.count: {entries.count()}')

min_entries = entries.filter(lambda vec: entryTypeIsMin(vec[1]))
print(f'entries.mins.count: {min_entries.count()} ( expect 730 )')

# create key-value tuple from (str,str,int) -> (str, int) ... key,value
station_temps = min_entries.map(lambda x: (x[0], x[2]))
print(station_temps.take(5))
print(f'count: {station_temps.count()}')

station_mins = station_temps.reduceByKey(keepMinOf)  # keep min for each station
print(f'count: {station_mins.count()}')

rs = station_mins.collect()
for (uuid, temp) in rs:
    print(f'{uuid} : {temp:.2f}')

from pyspark import SparkConf, SparkContext

"""
rdd.map() 
- executes fn once for each element of collection A
- N is unchanged
- A[] -> B[] : Na = Nb

rdd.flatMap()
- executes fn once for each element of collection A
- after execution the collection is "flattened" by a single level only
- A[] -> B[] : Nb > 0

"""

# configure
conf = SparkConf().setMaster('local').setAppName('friends_by_age')
sc = SparkContext(conf=conf)


def map_example(ctx):
    """creates a 2d list"""
    rdd = ctx.parallelize([1, 2, 3])
    rs = rdd.map(lambda x: [x, x * x])
    print(f'.map(): {rs.count()} : {rs.collect()}')


def flatmap_example(ctx):
    """xforms A[][] -> B[]"""
    rdd = ctx.parallelize([1, 2, 3])
    rs = rdd.flatMap(lambda x: [x, x * x])
    print(f'.flatMap(): {rs.count()} : {rs.collect()}')


def flatmap_only_flattens_single_level(ctx):
    """only flattens 1 level of the value"""
    rdd = ctx.parallelize([1, 2, 3])
    rs = rdd.flatMap(lambda x: [x, x * x, [x * x * x]])
    print(f'.flatMap(): {rs.count()} : {rs.collect()}')


def count_words_naive(ctx, path):
    """split lines into words, flatten, then count values"""
    rdd = ctx.textFile(path)
    words = rdd.flatMap(lambda x: x.split())
    print(words.take(20))
    wordcount = words.countByValue()
    print(f'words.countByValue(): count={len(wordcount)}')
    for word,count in wordcount.items():
        print(f'{word} : {count}')


map_example(sc)
flatmap_example(sc)
flatmap_only_flattens_single_level(sc)
count_words_naive(sc, './data/book.txt')

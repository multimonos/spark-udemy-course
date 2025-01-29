import re

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


def map_example(ctx: SparkContext):
    """creates a 2d list"""
    rdd = ctx.parallelize([1, 2, 3])
    rs = rdd.map(lambda x: [x, x * x])
    print(f'.map(): {rs.count()} : {rs.collect()}')


def flatmap_example(ctx: SparkContext):
    """xforms A[][] -> B[]"""
    rdd = ctx.parallelize([1, 2, 3])
    rs = rdd.flatMap(lambda x: [x, x * x])
    print(f'.flatMap(): {rs.count()} : {rs.collect()}')


def flatmap_only_flattens_single_level(ctx: SparkContext):
    """only flattens 1 level of the value"""
    rdd = ctx.parallelize([1, 2, 3])
    rs = rdd.flatMap(lambda x: [x, x * x, [x * x * x]])
    print(f'.flatMap(): {rs.count()} : {rs.collect()}')


def count_words_naive(ctx: SparkContext, path: str):
    """split lines into words, flatten, then count values"""
    rdd = ctx.textFile(path)
    words = rdd.flatMap(lambda x: x.split())  # str -> str[]
    wordcount = words.countByValue()  # str[] -> dict[str, int]
    print(f'words.naive.count(): count={len(wordcount)}')
    for word in words.take(10):
        print(word)


def count_words_regex(ctx: SparkContext, path: str, reg: re.Pattern):
    """word count, split using word regex"""
    rdd = ctx.textFile(path)
    words = rdd.flatMap(lambda x: reg.split(x.lower()))  # str -> str[]
    wordcount = words.countByValue()  # str[] -> dict[str, int]
    print(f'words.regex.count: reg={str(reg)}, count={len(wordcount)}')
    for word, count in wordcount.items():
        print(f'  {count}, {word}')


def count_words_sorted(ctx: SparkContext, path: str):
    """word counts, but, sorted by frequency"""
    rdd = ctx.textFile(path)
    reg = re.compile(r'\W+', re.UNICODE)

    # We could call .countByValue(), however, that returns a
    # dict which forces us into a python context.

    words = rdd.flatMap(lambda x: reg.split(x.lower()))
    tuples = words.map(lambda x: (x, 1))  # str[] -> (str, 1)[]
    words_count = tuples.reduceByKey(lambda acc, n: acc + n)  # count by key :  (str, 1)[] -> (str, int)[]
    flipped = words_count.map(lambda x: (x[1], x[0]))
    words_sorted = flipped.sortByKey()
    rs = words_sorted.collect()

    # output
    print(f'words.sorted.count: count={words_count.count()}')
    for cnt, word in rs:
        scnt = str(cnt).rjust(5)
        print(f'{scnt} : {word}')


"""
run the examples
"""
map_example(sc)
flatmap_example(sc)
flatmap_only_flattens_single_level(sc)
count_words_naive(sc, './data/book.txt')

"""
Examples using regex are slightly different ... `\b` splits on
the word boundary whereas `\W+` splits on a at least one non-word char
... the main difference here i think is that \W+ will collapse multiple
non-word chars into an empty string and return that as a word
"""
count_words_regex(sc, './data/book.txt', re.compile(r'\b', re.UNICODE))
count_words_regex(sc, './data/book.txt', re.compile(r'\W+', re.UNICODE))
count_words_sorted(sc, './data/book.txt')

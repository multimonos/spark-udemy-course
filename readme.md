# Spark Course

# Notes

## Choice of objects

- dataframe or rdd?
    - structured : dataframe
    - unstructured : maybe rdd is better

## RDD

Core of spark.

### RDD Transforms

RDD transforms do NOT modify the rdd in place. A new "copy" is returned.

Examples,

- map : execute a fn foreach entry; N is unchanged; fn() is 1:1
- flatmap : execute a fn foreach entry; N can change such
- filter : remove entries from dataset
- distinct : return unique values
- sample : take only a sample of all the values of a dataset
- union, intersection, subtract, cartesian : join / combine datasets
- mapValues(),flatMapValue()

### RDD Actions

No compute occurs on the RDD until an action is called ... uses lazy evaluation.

Examples,

- collect
- count
- countByValue
- take
- top
- reduce

## SparkSQL

- extends rdd to dataframe
- can use schema
- +rw json, hive,parquet, csv
- connect to jdbc/odbc, tableau
- create a `SparkSession` instead of `SparkContext`
- to create dataframe `df = spark.read.json(path/to/json)`
- `spark.sql(sql) -> dataframe`
- orm is available instead of sql commands
- convert `df -> rdd` is possible
- `dataframe` is the new "interface" we can rely on when consuming the data in other spark services
- exposes a data server via j/odbc
- user defined functions ( udf ) is possible for custom ops within a sql statement

### Chaining

Given that spark uses a lot of method chaining,

- filter first
- group similar ops
- transform then action, ie, `filter, map, groupBy` then `show, collect`

Break long chaings into multiple lines using

```python
(
    dataframe
    .filter((df.age >= 13) & (df.age <= 19))
    .orderBy('age')
    .show()
)
```

### Functions

- `split(delim)` : str column to array column
- `explode()` : array column to rows / flatten array column
- `withColumn(name, expr)` : add, replace, modify a column
- `lower()` : to lowercase
- `orderby(name)` : preferred for dataframe
- `sort(name)` :  alias of orderby for compat

### Dataframe vs. Dataset

- dataset are typed ... can wrap row data ... scala primary dtype
- dataframe are untyped ... row data ... python primary dtype

### Broadcast Variable

- sort of creates a global variable that is available on each node for consumption
- `spark.broadcast()` sends the variable to each executor
- broadcasted value can be consumed in any way ... oi

### Accumulator

- allow many executors to increment a shared variable  ... Q: race conds?

## Dataframe Caching
- any time you perform more than one Action on a df you should cache it to prevent re-evaluation
- use `.cache()` and `.persist()`
- `cache()` : caches to memory
- `persist()` : caches to disk; recovery possible; resource intensive

# Setup

- requires python 3.10.x

```bash
brew install openjdk@17
brew install scala
brew install apache-spark
```

# Changelog

- calculated histogram of movie rating values in datafile using `ratings-counter.py`
- movie dataset http://media.sundog-soft.com/es/ml-100k.zip
- system setup notes https://www.sundog-education.com/spark-python/
- installed apache-spark@3.5.4
- installed scala
- installed openjdk@17
- install python 3.10.11

# Scala

- installed via `brew install scala`
- after install brew said

```
To use with IntelliJ, set the Scala home to:
/opt/homebrew/opt/scala/idea
```



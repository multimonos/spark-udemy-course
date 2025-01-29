# Spark Course

# Notes

## RDD
Core of spark.

### RDD Transforms
RDD transforms do NOT modify the rdd in place.  A new "copy" is returned.

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

To use with IntelliJ, set the Scala home to:
/opt/homebrew/opt/scala/idea



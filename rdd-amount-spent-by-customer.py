from pyspark import SparkConf, SparkContext


def createOrder(raw: str):  # str -> (str,float)
    data = raw.split(',')
    cid = str(data[0])
    cost = float(data[2])
    return cid, cost


def swapPair(x):
    return x[1], x[0]


if __name__ == "__main__":
    # config
    conf = SparkConf().setMaster('local').setAppName('friends_by_age')
    sc = SparkContext(conf=conf)

    # load
    rdd = sc.textFile('./data/customer-orders.csv')
    print(f'count: {rdd.count()}')

    # create an Order tuple
    orders = rdd.map(createOrder)  # str[] -> (str, float)[]
    print(f'pairs: {orders.take(3)}')

    # sum amount spent by each Customer
    customer_totals = orders.reduceByKey(lambda acc, cost: acc + cost)  # (str,float)[] -> (str,float)[]
    print(f'totals: {customer_totals.take(3)}')

    # sort by total asc
    sorted_totals = customer_totals.map(swapPair).sortByKey()

    # display
    rs = sorted_totals.collect()

    for total, cid in rs:
        print(f'{total:,.2f} : {cid}')

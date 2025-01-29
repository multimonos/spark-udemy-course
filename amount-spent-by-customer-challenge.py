from pyspark import SparkConf, SparkContext


def createOrder(raw: str):  # str -> (str,float)
    data = raw.split(',')
    cid = str(data[0])
    cost = float(data[2])
    return cid, cost


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
    customerTotals = orders.reduceByKey(lambda acc, cost: acc + cost)  # (str,float)[] -> (str,float)[]
    print(f'totals: {customerTotals.take(3)}')

    # display
    rs = customerTotals.sortByKey().collect()

    for cid, total in rs:
        print(f'{cid} : {total:,.2f}')

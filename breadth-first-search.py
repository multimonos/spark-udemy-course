"""
using the marvel-graph.txt file to calculate degrees of separation between 2 characters

csv
- file is 2 or more integers separated by spaces
- first_num: hero_id
- other_nums: ids of heroes this hero has appeared with in comic books

schema
- hero_id
- hero_conn_ids

example
- 5983 1165 3836 4361 1282

for bfs search we will,
 - transform each line to (5983, (1165,3836,4361,1282), 9999, WHITE)
 - aka (hero_id, (...hero_ids, distance, node_status)
 - initially nodes are considered infinitely separated such that distance=9999
 - undiscovered nodes have node_status=WHITE
"""
import sys
from typing import Final, List, NamedTuple, Tuple

from pyspark import Accumulator, RDD, SparkConf, SparkContext

# const
UNKNOWN_DISTANCE: Final = 9999
ZERO_DISTANCE: Final = 0

# const/node_status
UNVISITED_NODE: Final = 1  # white: initial state
DISCOVERED_NODE: Final = 2  # grey: queued for processing; adjacent nodes undisccovered
PROCESSED_NODE: Final = 3  # black: fully explored; all adjacent nodes have been discovered


# more readable, but, slightly less performant
class HeroNode(NamedTuple):
    conns: List[int]
    distance: int
    status: int
    is_entry_node: bool = False
    is_exit_node: bool = False


def evole_row_for_bfs(line: str) -> Tuple[int, HeroNode]:
    fields = line.split()
    hero_id = int(fields[0])
    conns = [int(x) for x in fields[1:]]
    status = UNVISITED_NODE
    distance = UNKNOWN_DISTANCE

    return hero_id, HeroNode(conns, distance, status)


def load_hero_graph(ctx: SparkContext, path: str) -> RDD:
    return (
        ctx
        .textFile(path)
        .map(evole_row_for_bfs)
    )


def status_report(rows: RDD) -> dict[str, int]:
    unvisited = rows.filter(lambda vec: vec[1].status == UNVISITED_NODE).count()
    discovered = rows.filter(lambda vec: vec[1].status == DISCOVERED_NODE).count()
    processed = rows.filter(lambda vec: vec[1].status == PROCESSED_NODE).count()
    exit_nodes = rows.filter(lambda vec: vec[1].is_exit_node).count()
    entry_nodes = rows.filter(lambda vec: vec[1].is_entry_node).count()
    report = {
        "unvisited": unvisited,
        "discovered": discovered,
        "processed": processed,
        "entry_nodes": entry_nodes,
        "exit_nodes": exit_nodes,
    }
    return report


def explode_node(exit_id: int, row: Tuple[int, HeroNode]) -> List[Tuple[int, HeroNode]]:
    rs = []

    hero_id, node = row

    if node.status == DISCOVERED_NODE:

        # create new nodes to be processed
        for hid in node.conns:
            # is this the node we have been looking for
            is_exit_id = True if exit_id == hid else False

            # if is_exit_id:
            #     print(f'found exit_id: {exit_id}')

            # print(f'\n{hid}')
            cnode = hid, HeroNode(status=DISCOVERED_NODE, conns=[], distance=node.distance + 1,
                                  is_exit_node=is_exit_id)
            rs.append(cnode)

        # mark original as processed
        nrow = (hero_id, HeroNode(status=PROCESSED_NODE, conns=node.conns, distance=node.distance,
                                  is_entry_node=node.is_entry_node, is_exit_node=node.is_exit_node))
        rs.append(nrow)

    else:  # do nothing
        rs.append(row)

    return rs


def bfs_reduce(n0: HeroNode, n1: HeroNode) -> HeroNode:
    return HeroNode(
        conns=n0.conns + n1.conns,
        distance=min(n0.distance, n1.distance),
        status=max(n0.status, n1.status),
        is_entry_node=n0.is_entry_node or n1.is_entry_node,
        is_exit_node=n0.is_exit_node or n1.is_exit_node,
    )


def get_target_rows(graph: RDD) -> List[Tuple[int, HeroNode]]:
    return graph.filter(lambda x: x[1].is_exit_node).collect() # this might be a processing mistake


def degrees_of_separation(graph: RDD, hit_counter: Accumulator, entry_id: int, exit_id: int) -> int:
    """
    - iterate a fixed number of times instead of using recursion ... which is probably dangerous for bigdata

    loop 0
    - is really just looking for the start_id
    - processes every original node

    loop 1+ looks like,
    - for each node in the graph we create a new node from the connections list such that
        - node.status = discovered
        - node.conns = [] ... empty list
        - node.distance++
        - add this new node to the graph
        - each of these nodes will be visited on the next iteration and perhaps one of them is the finish_id
    - mark currentNode as node.status = processed
    """

    # [print(x) for x in graph.collect()]

    for i in range(0, 10):

        print(f'\n---  iter {i}  ---')

        mapped = graph.flatMap(lambda row: explode_node(exit_id, row))

        print(f'status: {status_report(mapped)}')

        # display the count so that the accumulator is evaluated
        print(f'node.count: {mapped.count()}')

        # update the graph
        graph = mapped.reduceByKey(bfs_reduce)

        # check the status
        status = status_report(graph)

        if status['exit_nodes'] > 0:
            print(f'target found iter={i}')
            rows = get_target_rows(graph)
            return min([node.distance for k, node in rows])

    return -1


def init_start_node(search_id: int, row: Tuple[int, HeroNode]) -> Tuple[int, HeroNode]:
    hero_id, node = row

    if search_id == hero_id:
        new_node = hero_id, HeroNode(status=DISCOVERED_NODE, distance=ZERO_DISTANCE, conns=node.conns,
                                     is_entry_node=True, is_exit_node=False)
        return new_node

    return row


if __name__ == '__main__':
    # config
    conf = SparkConf().setMaster('local')
    sc = SparkContext(conf=conf)

    # data
    hit_counter = sc.accumulator(0)
    graph = load_hero_graph(sc, './data/marvel-graph.txt')
    print(f'count: {graph.count()}')

    # mark the entry node
    entry_id = 5306  # Spiderman
    exit_id = 14  # ADAM

    initialized = graph.map(lambda row: init_start_node(entry_id, row))

    # validate entry node found
    status = status_report(initialized)
    print(f'status: {status}')

    if status['discovered'] == 0:
        print(f'Search failed: Unable to find an entry node with hero_id={entry_id}')
        sys.exit(1)
    else:
        print(f'entry_nodes: {status["discovered"]}')

    # calculate the degrees of separation from entry_id -> exit_id in the graph
    dos = degrees_of_separation(initialized, hit_counter, entry_id, exit_id)
    if dos > 0:
        print(f'Success! {entry_id} is {dos} degrees of separation from {exit_id}')
    else:
        print(f'Failed.')

    # exit
    sc.stop()

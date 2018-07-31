import logging
import queue
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import networkx as nx
import psycopg2.extras

from exceptions import ExecutionException
from graph import AWSGraphBuilder, ExecutionGraph
from tasks import TaskAction, TaskState, CreateInstance

log = logging.getLogger(__name__)

log.setLevel(logging.DEBUG)
ch = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter('%(levelname)s - %(message)s')
ch.setFormatter(formatter)
log.addHandler(ch)

connection = psycopg2.connect(dbname='testgraphdb')
cursor = connection.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor)
builder = AWSGraphBuilder(connection)

# g = builder.create('test cluster', num_nodes=1, num_dcs=1)
# connection.commit()
#
cluster_id = '2aa9f0e3-84cb-4084-bcf2-f30a47c50dc3'
g = builder.load(cluster_id)

graph = ExecutionGraph(g)
graph.draw('/tmp/test.jpg')

print(graph.info())

import boto3

# Let's use Amazon S3
ec2 = boto3.resource('ec2')


for node in graph.graph.nodes():
    if type(node) is CreateInstance:
        print(list(nx.bfs_successors(graph.graph, node)))


# print(graph.provisioning_tasks())


sys.exit()

result_queue = queue.Queue()


def new_cursor():
    return connection.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor)


class Worker(threading.Thread):

    def __init__(self, result_queue, connection):
        super(Worker, self).__init__()
        self._stop_event = threading.Event()
        self.queue = result_queue
        self.connection = connection

    def run(self):
        while not self.stopped():
            try:
                result = self.queue.get(timeout=1).result()
                if result.state == TaskState.PROVISIONED:
                    log.info("Completed task: {}".format(result))
            except queue.Empty:
                pass
            except ExecutionException as e:
                log.error("{} - {}".format(e.task, e.exception))
            finally:
                self.connection.commit()

    def stop(self):
        self._stop_event.set()

    def stopped(self):
        return self._stop_event.is_set()


def on_done(future):
    result_queue.put(future)


pool = ThreadPoolExecutor(100)
worker = Worker(result_queue, connection)
worker.start()

while True:
    print(graph.info())

    provisioning_tasks = graph.provisioning_tasks()
    deletion_tasks = graph.deletion_tasks()

    if provisioning_tasks:
        for task in provisioning_tasks:
            pool.submit(task, new_cursor(), ec2, TaskAction.PROVISION).add_done_callback(on_done)
    elif deletion_tasks:
        for task in deletion_tasks:
            pool.submit(task, new_cursor(), TaskAction.DELETE).add_done_callback(on_done)

    time.sleep(1)

worker.stop()

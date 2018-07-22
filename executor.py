import logging
import queue
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import psycopg2.extras

from exceptions import ExecutionException
from graph import AWSGraphBuilder, ExecutionGraph, TaskAction, TaskState

log = logging.getLogger(__name__)

log.setLevel(logging.DEBUG)
ch = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter('%(levelname)s - %(message)s')
ch.setFormatter(formatter)
log.addHandler(ch)

connection = psycopg2.connect(dbname='testgraphdb')
cursor = connection.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor)
builder = AWSGraphBuilder(connection)

# cursor.execute("""
#     INSERT INTO cluster(name)
#     VALUES ('ass')
#     RETURNING id
# """)
#
# cluster_id = cursor.fetchone().id
# print(cluster_id)
#
# g = builder.create(cluster_id, num_nodes=1, num_dcs=1)
# connection.commit()

cluster_id = '81460476-2727-473a-a7da-375bc69fbe65'
g = builder.load(cluster_id)

graph = ExecutionGraph(g)
# graph.draw('/tmp/test.jpg')

print(graph.root.can_delete)

# print(graph.provisioning_tasks())
print(graph.deletion_tasks())

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
            pool.submit(task, new_cursor(), TaskAction.PROVISION).add_done_callback(on_done)
    elif deletion_tasks:
        for task in deletion_tasks:
            pool.submit(task, new_cursor(), TaskAction.DELETE).add_done_callback(on_done)

    time.sleep(1)

    for failed_node in graph.nodes_for_state(TaskState.FAILED):
        failed_node.retry_failed()

worker.stop()

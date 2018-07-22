import queue
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import psycopg2.extras

from graph import AWSGraphBuilder, ExecutionGraph, State
from tasks import ExecutionException

connection = psycopg2.connect(dbname='testgraphdb')
cursor = connection.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor)
builder = AWSGraphBuilder(connection)

cursor.execute("""
    INSERT INTO cluster(name)
    VALUES ('ass')
    RETURNING id
""")

cluster_id = cursor.fetchone().id
g = builder.create(cluster_id, num_nodes=3, num_dcs=2)

graph = ExecutionGraph(g)
graph.draw('/tmp/test.jpg')

result_queue = queue.Queue()


class Worker(threading.Thread):

    def __init__(self, result_queue):
        super(Worker, self).__init__()
        self._stop_event = threading.Event()
        self.queue = result_queue

    def run(self):
        while not self.stopped():
            try:
                result = self.queue.get(timeout=1).result()
                print(result)
            except queue.Empty:
                pass
            except ExecutionException as e:
                print("ERROR: {} - {}".format(e.task, e.exception))
                pass

    def stop(self):
        self._stop_event.set()

    def stopped(self):
        return self._stop_event.is_set()


def on_done(future):
    result_queue.put(future)


pool = ThreadPoolExecutor(100)
worker = Worker(result_queue)
worker.start()

while not graph.complete():
    print(graph.info())

    for task in graph.runnable_tasks():
        pool.submit(task).add_done_callback(on_done)

    time.sleep(1)

    for failed_node in graph.nodes_for_state(State.FAILED):
        failed_node.retry_failed()

worker.stop()

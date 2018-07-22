import queue
import threading
from concurrent.futures import ThreadPoolExecutor

from graph import AWSGraph
from tasks import *

pool = ThreadPoolExecutor(100)

task_graph = AWSGraph(3, 2)

task_graph.draw('/tmp/test-cluster.jpg')

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


worker = Worker(result_queue)
worker.start()

while not task_graph.complete():
    print(task_graph.percent_complete(), end=' ')
    task_graph.info()
    for task in task_graph.runnable_tasks():
        pool.submit(task).add_done_callback(on_done)

    time.sleep(1)

    for failed_node in task_graph.nodes_for_state(State.FAILED):
        failed_node.reset_failed()

worker.stop()

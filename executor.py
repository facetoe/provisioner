from concurrent.futures import ThreadPoolExecutor

from graph import AWSGraph
from tasks import *

pool = ThreadPoolExecutor(10)

cluster = AWSGraph(3, 1)

cluster.draw('/tmp/test-cluster.jpg')

while not cluster.provisioned():
    futures = list()
    for task in cluster.runnable_tasks():
        task.state = State.RUNNING
        futures.append(pool.submit(task))

    time.sleep(1)

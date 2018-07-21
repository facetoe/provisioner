from concurrent.futures import ThreadPoolExecutor

from graph import AWSGraph
from tasks import *

pool = ThreadPoolExecutor(10)

cluster = AWSGraph(3, 2)

cluster.draw('/tmp/test-cluster.jpg')

# while not cluster.provisioned():
#     for task in cluster.runnable_tasks():
#         task.state = State.RUNNING
#         try:
#             pool.submit(task)
#         except Exception as e:
#             task.state = State.FAILED
#             print(e)
#     time.sleep(1)

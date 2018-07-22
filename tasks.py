import json
import time
from abc import abstractmethod
from enum import Enum
from random import randint


class ExecutionException(Exception):
    def __init__(self, task, exception):
        super().__init__()
        self.task = task
        self.exception = exception


class State(Enum):
    PENDING = 'PENDING'
    EXECUTING = 'EXECUTING'
    COMPLETE = 'COMPLETE'
    FAILED = 'FAILED'


class Task:
    def __init__(self, graph, task_id=None):
        self.graph = graph
        self.id = task_id
        self._state = State.PENDING

    @abstractmethod
    def run(self):
        if randint(0, 10) % 5 == 0:
            raise Exception("I FAILED")
        time.sleep(randint(0, 10))
        self._state = State.COMPLETE
        pass

    def __call__(self, *args, **kwargs):
        try:
            self._state = State.EXECUTING
            self.run()
            return self
        except Exception as e:
            self._state = State.FAILED
            raise ExecutionException(self, e)

    def __str__(self):
        return repr(self)

    def __repr__(self):
        return "{}(id='{}')".format(type(self).__name__, self.id)

    def persist(self, cursor, cluster_id):
        cursor.execute("""
            INSERT INTO node (type, payload, cluster)
            VALUES (%(type)s, %(payload)s, %(cluster)s) 
            RETURNING id
        """, dict(type=type(self).__name__, payload=json.dumps({}), cluster=cluster_id))
        self.id = cursor.fetchone().id
        return self

    @property
    def runnable(self):
        if self._state != State.PENDING:
            return False
        elif len(list(self.predecessors())) == 0:
            return True
        else:
            return all((s.state == State.COMPLETE for s in self.predecessors()))

    def successors(self):
        return self.graph.successors(self)

    def predecessors(self):
        return self.graph.predecessors(self)

    def retry_failed(self):
        self._state = State.PENDING

    def failed(self):
        return self._state == State.FAILED

    def running(self):
        return self._state == State.FAILED

    def pending(self):
        return self._state == State.PENDING

    def complete(self):
        return self._state == State.COMPLETE

    @property
    def state(self):
        return self._state


class DataCentre(Task):
    pass


class Cluster(Task):
    pass


class Role(Task):
    pass


class VPC(Task):
    pass


class SecurityGroups(Task):
    pass


class BindSecurityGroup(Task):
    pass


class InternetGateway(Task):
    pass


class RouteTable(Task):
    pass


class SubNets(Task):
    pass


class FirewallRules(Task):
    pass


class CreateEBS(Task):
    pass


class AttachEBS(Task):
    pass


class Nodes(Task):
    pass


class CreateInstance(Task):
    pass


class BindIP(Task):
    pass

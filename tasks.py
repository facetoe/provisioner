import json
import time
from abc import abstractmethod
from enum import Enum
from random import randint

from exceptions import ExecutionException


class TaskState(Enum):
    PENDING = 'PENDING'
    EXECUTING = 'EXECUTING'
    COMPLETE = 'COMPLETE'
    FAILED = 'FAILED'


class Task:
    def __init__(self, graph, task_id=None, task_state=None):
        self.graph = graph
        self.id = task_id
        self._state = TaskState(task_state) or TaskState.PENDING

    @abstractmethod
    def run(self, cursor):
        if randint(0, 100) % 10 == 0:
            raise Exception("I FAILED")
        time.sleep(randint(0, 10))
        self.set_state(cursor, TaskState.COMPLETE)
        pass

    def __call__(self, cursor):
        try:
            self.set_state(cursor, TaskState.EXECUTING)
            self.run(cursor)
            return self
        except Exception as e:
            self.set_state(cursor, TaskState.FAILED)
            raise ExecutionException(self, e)

    def __str__(self):
        return repr(self)

    def __repr__(self):
        return "{}(id='{}')".format(type(self).__name__, self.id)

    def persist(self, cursor, cluster_id):
        if self.id is not None:
            raise Exception("This task is already persisted!")
        cursor.execute("""
            INSERT INTO node (type, payload, cluster)
            VALUES (%(type)s, %(payload)s, %(cluster)s) 
            RETURNING id
        """, dict(type=type(self).__name__, payload=json.dumps({}), cluster=cluster_id))
        self.id = cursor.fetchone().id
        return self

    def set_state(self, cursor, state):
        cursor.execute("""
            UPDATE node
            SET state = %(state)s
            WHERE id = %(id)s
        """, dict(state=state.name, id=self.id))
        self._state = state

    @property
    def runnable(self):
        if self._state != TaskState.PENDING:
            return False
        elif len(list(self.predecessors())) == 0:
            return True
        else:
            return all((s.state == TaskState.COMPLETE for s in self.predecessors()))

    def successors(self):
        return self.graph.successors(self)

    def predecessors(self):
        return self.graph.predecessors(self)

    def retry_failed(self):
        self._state = TaskState.PENDING

    def failed(self):
        return self._state == TaskState.FAILED

    def running(self):
        return self._state == TaskState.FAILED

    def pending(self):
        return self._state == TaskState.PENDING

    def complete(self):
        return self._state == TaskState.COMPLETE

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

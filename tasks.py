import json
import time
from abc import abstractmethod
from enum import Enum
from random import randint

from exceptions import ExecutionException


class TaskState(Enum):
    PENDING_PROVISION = 'PENDING_PROVISION'
    PROVISIONING = 'PROVISIONING'
    PROVISIONED = 'PROVISIONED'
    PENDING_DELETION = 'PENDING_DELETION'
    DELETING = 'DELETING'
    DELETED = 'DELETED'
    FAILED = 'FAILED'


class TaskAction(Enum):
    DELETE = 'DELETE'
    PROVISION = 'PROVISION'


class Task:
    def __init__(self, graph, task_id=None, task_state=None):
        self.graph = graph
        self.id = task_id
        self._state = TaskState(task_state) if task_state else TaskState.PENDING_PROVISION

    @abstractmethod
    def provision(self, cursor):
        if randint(0, 100) % 10 == 0:
            raise Exception("I FAILED")
        time.sleep(randint(0, 10))
        self.set_state(cursor, TaskState.PROVISIONED)
        pass

    @abstractmethod
    def delete(self, cursor):
        time.sleep(randint(0, 1))
        self.set_state(cursor, TaskState.DELETED)

    def __call__(self, cursor, action):
        try:
            if action == TaskAction.PROVISION:
                self.set_state(cursor, TaskState.PROVISIONING)
                self.provision(cursor)
            elif action == TaskAction.DELETE:
                self.set_state(cursor, TaskState.DELETING)
                self.delete(cursor)
            else:
                raise Exception("Invalid task action: {}".format(action))
            return self
        except Exception as e:
            self.set_state(cursor, TaskState.FAILED)
            raise ExecutionException(self, e)

    def __str__(self):
        return repr(self)

    def __repr__(self):
        return "{}(id='{}', state='{}')".format(type(self).__name__, self.id, str(self.state))

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
    def can_provision(self):
        if self._state != TaskState.PENDING_PROVISION:
            return False
        elif len(list(self.predecessors())) == 0:
            return True
        else:
            return all((s.state == TaskState.PROVISIONED for s in self.predecessors()))

    @property
    def can_delete(self):
        if self._state != TaskState.PENDING_DELETION:
            return False
        elif len(list(self.successors())) == 0:
            return True
        elif all((s.state == TaskState.DELETED for s in self.successors())):
            return True
        return False

    def successors(self):
        return self.graph.successors(self)

    def predecessors(self):
        return self.graph.predecessors(self)

    def retry_failed(self):
        self._state = TaskState.PENDING_PROVISION

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

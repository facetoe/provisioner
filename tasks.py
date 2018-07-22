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
    def __init__(self, graph):
        self.graph = graph
        self.id = "{}-{}".format(type(self).__name__, id(self))
        self.state = State.PENDING

    @abstractmethod
    def run(self):
        if randint(0, 10) % 5 == 0:
            raise Exception("I FAILED")
        time.sleep(randint(0, 10))
        self.state = State.COMPLETE
        pass

    def __call__(self, *args, **kwargs):
        try:
            self.state = State.EXECUTING
            self.run()
            return self
        except Exception as e:
            self.state = State.FAILED
            raise ExecutionException(self, e)

    def __str__(self):
        return self.id

    def __repr__(self):
        return "{}(id='{}', runnable={}, state={})".format(
            type(self).__name__, self.id, self.runnable, self.state)

    @property
    def runnable(self):
        if self.state != State.PENDING:
            return False
        elif len(list(self.predecessors())) == 0:
            return True
        else:
            return all((s.state == State.COMPLETE for s in self.predecessors()))

    def successors(self):
        return self.graph.successors(self)

    def predecessors(self):
        return self.graph.predecessors(self)

    def reset_failed(self):
        self.state = State.PENDING

    def failed(self):
        return self.state == State.FAILED

    def running(self):
        return self.state == State.FAILED

    def pending(self):
        return self.state == State.PENDING


class DataCentre(Task):
    def __init__(self, graph):
        super().__init__(graph)


class Cluster(Task):
    def __init__(self, graph):
        super().__init__(graph)


class Role(Task):
    def __init__(self, graph):
        super().__init__(graph)


class VPC(Task):
    def __init__(self, graph):
        super().__init__(graph)


class SecurityGroups(Task):
    def __init__(self, graph):
        super().__init__(graph)


class BindSecurityGroup(Task):
    def __init__(self, graph):
        super().__init__(graph)


class InternetGateway(Task):
    def __init__(self, graph):
        super().__init__(graph)


class RouteTable(Task):
    def __init__(self, graph):
        super().__init__(graph)


class SubNets(Task):
    def __init__(self, graph):
        super().__init__(graph)


class FirewallRules(Task):
    def __init__(self, graph):
        super().__init__(graph)


class CreateEBS(Task):
    def __init__(self, graph):
        super().__init__(graph)


class AttachEBS(Task):
    def __init__(self, graph):
        super().__init__(graph)


class Nodes(Task):
    def __init__(self, graph):
        super().__init__(graph)


class CreateInstance(Task):
    def __init__(self, graph):
        super().__init__(graph)


class BindIP(Task):
    def __init__(self, graph):
        super().__init__(graph)

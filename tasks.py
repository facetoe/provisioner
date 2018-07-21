import time
from enum import Enum
from random import randint


class State(Enum):
    PENDING = 'PENDING'
    RUNNING = 'RUNNING'
    COMPLETE = 'COMPLETE'
    FAILED = 'FAILED'


class Task:
    def __init__(self, graph, name):
        self.graph = graph
        self.id = "{}-{}".format(name, id(self))
        self.state = State.PENDING

    def __call__(self, *args, **kwargs):
        print(" {} - running".format(self.id))
        time.sleep(randint(1, 5))
        print(" {} - complete".format(self.id))
        self.state = State.COMPLETE
        return self

    def __str__(self):
        return self.id

    def __repr__(self):
        return "{}(id='{}', runnable={}, state={})".format(type(self).__name__, self.id, self.runnable,
                                                           self.state)

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


class DataCentre(Task):
    def __init__(self, graph):
        super().__init__(graph, 'datacentre')


class Cluster(Task):
    def __init__(self, graph):
        super().__init__(graph, 'cluster')


class Role(Task):
    def __init__(self, graph):
        super().__init__(graph, 'role')


class VPC(Task):
    def __init__(self, graph):
        super().__init__(graph, 'vpc')


class SecurityGroups(Task):
    def __init__(self, graph):
        super().__init__(graph, 'security_groups')


class BindSecurityGroup(Task):
    def __init__(self, graph):
        super().__init__(graph, 'bind_security_group')


class InternetGateway(Task):
    def __init__(self, graph):
        super().__init__(graph, 'internet_gateway')


class RouteTable(Task):
    def __init__(self, graph):
        super().__init__(graph, 'route_table')


class SubNets(Task):
    def __init__(self, graph):
        super().__init__(graph, 'subnets')


class FirewallRules(Task):
    def __init__(self, graph):
        super().__init__(graph, 'firewall_rules')


class CreateEBS(Task):
    def __init__(self, graph):
        super().__init__(graph, 'create_ebs')


class AttachEBS(Task):
    def __init__(self, graph):
        super().__init__(graph, 'attach_ebs')


class Nodes(Task):
    def __init__(self, graph):
        super().__init__(graph, 'nodes')


class CreateInstance(Task):
    def __init__(self, graph):
        super().__init__(graph, 'create_instance')


class BindIP(Task):
    def __init__(self, graph):
        super().__init__(graph, 'bind_ip')
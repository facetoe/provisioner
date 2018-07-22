import logging
from collections import namedtuple
from typing import Tuple

import networkx as nx
import psycopg2
import psycopg2.extras

from tasks import *

Edge = namedtuple("Edge", "from_node to_node")

log = logging.getLogger(__name__)


class GraphBuilder:
    def __init__(self, connection):
        self.connection = connection
        self.cursor = self.connection.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor)

    @abstractmethod
    def create(self, cluster_id, num_nodes, num_dcs):
        pass

    def load(self, cluster_id):
        execution_graph = nx.DiGraph()
        node_map = self.build_node_map(cluster_id, execution_graph)
        edges = self.get_edges(cluster_id, node_map)
        execution_graph.add_edges_from(edges)
        return execution_graph

    def build_node_map(self, cluster_id, task_graph):
        self.cursor.execute("""
            SELECT *
            FROM node
            WHERE cluster = %(cluster)s
        """, dict(cluster=cluster_id))
        type_map = dict()
        for task_type in Task.__subclasses__():
            type_map[task_type.__name__] = task_type

        node_map = dict()
        for row in list(self.cursor):
            node = type_map[row.type](graph=task_graph, task_id=row.id, task_state=row.state)
            if node.state == TaskState.EXECUTING:
                log.warning("Node: {} was in the EXECUTING state on load, setting to FAILED".format(node))
                node.set_state(self.cursor, TaskState.FAILED)
            node_map[node.id] = node
        return node_map

    def get_edges(self, cluster_id, node_map):
        self.cursor.execute("""
             WITH RECURSIVE search_graph(from_node, to_node, id, cluster, depth, path, cycle)
                AS (
                    SELECT e.from_node, e.to_node, e.id, e.cluster, 1,
                    ARRAY[e.id],
                    false
                FROM edge e
                WHERE e.cluster = %(cluster)s
                UNION ALL
                SELECT e.from_node, e.to_node, e.id, e.cluster, sg.depth + 1,  path || e.id, e.id = ANY(path)
                FROM edge e, search_graph sg
                WHERE e.from_node = sg.to_node AND NOT cycle AND e.cluster = %(cluster)s
            )
            SELECT DISTINCT ON(from_node, to_node) * FROM search_graph;
        """, dict(cluster=cluster_id))

        edges = set()
        for row in self.cursor:
            edges.add(Edge(from_node=node_map[row.from_node], to_node=node_map[row.to_node]))
        return edges


class AWSGraphBuilder(GraphBuilder):

    def create(self, cluster_id, num_nodes, num_dcs):
        task_graph = nx.DiGraph()

        def persisted(task_obj):
            return task_obj.persist(self.cursor, cluster_id)

        cluster = persisted(Cluster(task_graph))
        for _ in range(num_dcs):
            dc = persisted(DataCentre(task_graph))
            role = persisted(Role(task_graph))
            vpc = persisted(VPC(task_graph))
            security_groups = persisted(SecurityGroups(task_graph))
            internet_gateway = persisted(InternetGateway(task_graph))
            route_table = persisted(RouteTable(task_graph))
            subnets = persisted(SubNets(task_graph))
            firewall_rules = persisted(FirewallRules(task_graph))

            cluster_edges = [
                Edge(cluster, dc),
                Edge(dc, role),
                Edge(dc, vpc),
                Edge(vpc, security_groups),
                Edge(vpc, internet_gateway),
                Edge(vpc, route_table),
                Edge(internet_gateway, route_table),
                Edge(vpc, subnets),
                Edge(vpc, security_groups),
                Edge(security_groups, firewall_rules),
            ]

            for n in range(num_nodes):
                create_instance = persisted(CreateInstance(task_graph))
                create_ebs = persisted(CreateEBS(task_graph))
                attach_ebs = persisted(AttachEBS(task_graph))
                bind_security_group = persisted(BindSecurityGroup(task_graph))
                bind_ip = persisted(BindIP(task_graph))

                cluster_edges.append(Edge(dc, create_instance))
                cluster_edges.append(Edge(dc, create_ebs))

                cluster_edges.append(Edge(create_ebs, attach_ebs))
                cluster_edges.append(Edge(create_instance, attach_ebs))
                cluster_edges.append(Edge(create_instance, bind_ip))

                cluster_edges.append(Edge(create_instance, bind_security_group))
                cluster_edges.append(Edge(security_groups, bind_security_group))

            task_graph.add_edges_from(cluster_edges)

            for edge in cluster_edges:
                self.cursor.execute("""
                    INSERT INTO edge (from_node, to_node, cluster)
                    VALUES (%(from_node)s, %(to_node)s, %(cluster)s)
                """, dict(from_node=edge.from_node.id, to_node=edge.to_node.id, cluster=cluster_id))
            return task_graph


class ExecutionGraph:
    def __init__(self, graph):
        self.graph = graph
        self.root = next(nx.topological_sort(graph))

    @abstractmethod
    def build(self, num_nodes: int, num_dcs: int) -> Tuple[nx.DiGraph, Task]:
        pass

    def runnable_tasks(self):
        return self._gather_runnable(self.root)

    def _gather_runnable(self, root):
        runnable = set()
        if root.runnable:
            runnable.add(root)
        for node in self.graph.successors(root):
            if node.runnable:
                runnable.add(node)
            runnable.update(self._gather_runnable(node))
        return runnable

    def complete(self):
        for node in self.graph.nodes():
            if not node.complete():
                return False
        return True

    def nodes_for_state(self, state):
        nodes = []
        for node in self.graph.nodes():
            if node.state == state:
                nodes.append(node)
        return nodes

    def info(self):
        pending = len(self.nodes_for_state(TaskState.PENDING))
        failed = len(self.nodes_for_state(TaskState.FAILED))
        complete = len(self.nodes_for_state(TaskState.COMPLETE))
        excecuting = len(self.nodes_for_state(TaskState.EXECUTING))
        return "{:.2f}% done:  Pending: {}, Failed: {}, Complete: {}, Excecuting: {}" \
            .format(self.percent_complete(), pending, failed, complete, excecuting)

    def percent_complete(self):
        return len(self.nodes_for_state(TaskState.COMPLETE)) * 100 / len(self.graph)

    def draw(self, path):
        agraph = nx.nx_agraph.to_agraph(self.graph)
        agraph.layout('dot', args='-Nfontsize=10 -Nwidth=".2" -Nheight=".2" -Nmargin=0 -Gfontsize=8')
        agraph.draw(path)

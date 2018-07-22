from collections import namedtuple
from typing import Tuple

import networkx as nx
import psycopg2
import psycopg2.extras

from tasks import *

Edge = namedtuple("Edge", "from_node to_node")


class GraphBuilder:

    def __init__(self, connection):
        self.connection = connection
        self.cursor = self.connection.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor)

    def load(self, cluster_id):
        task_graph = nx.DiGraph()

        node_map = self.build_node_map(cluster_id, task_graph)
        edges = self.get_edges(cluster_id, node_map)
        task_graph.add_edges_from(edges)
        return task_graph

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

    def build_node_map(self, cluster_id, task_graph):
        self.cursor.execute("""
            SELECT *
            FROM node
            WHERE cluster = %(cluster)s
        """, dict(cluster=cluster_id))
        type_map = dict()
        for clazz in Task.__subclasses__():
            type_map[clazz.__name__] = clazz
        node_map = dict()
        for row in self.cursor:
            node = type_map[row.type](graph=task_graph, task_id=row.id)
            node_map[node.id] = node
        return node_map

    def create(self, cluster_id, num_nodes, num_dcs):
        task_graph = nx.DiGraph()

        cluster = Cluster(task_graph).persist(self.cursor, cluster_id)
        for _ in range(num_dcs):
            dc = DataCentre(task_graph).persist(self.cursor, cluster_id)
            role = Role(task_graph).persist(self.cursor, cluster_id)
            vpc = VPC(task_graph).persist(self.cursor, cluster_id)
            security_groups = SecurityGroups(task_graph).persist(self.cursor, cluster_id)
            internet_gateway = InternetGateway(task_graph).persist(self.cursor, cluster_id)
            route_table = RouteTable(task_graph).persist(self.cursor, cluster_id)
            subnets = SubNets(task_graph).persist(self.cursor, cluster_id)
            firewall_rules = FirewallRules(task_graph).persist(self.cursor, cluster_id)

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
                create_instance = CreateInstance(task_graph).persist(self.cursor, cluster_id)
                create_ebs = CreateEBS(task_graph).persist(self.cursor, cluster_id)
                attach_ebs = AttachEBS(task_graph).persist(self.cursor, cluster_id)
                bind_security_group = BindSecurityGroup(task_graph).persist(self.cursor, cluster_id)

                cluster_edges.append(Edge(dc, create_instance))
                cluster_edges.append(Edge(dc, create_ebs))

                cluster_edges.append(Edge(create_ebs, attach_ebs))
                cluster_edges.append(Edge(create_instance, attach_ebs))
                cluster_edges.append(Edge(create_instance, BindIP(task_graph).persist(self.cursor, cluster_id)))

                cluster_edges.append(Edge(create_instance, bind_security_group))
                cluster_edges.append(Edge(security_groups, bind_security_group))

            task_graph.add_edges_from(cluster_edges)

            for edge in cluster_edges:
                self.cursor.execute("""
                    INSERT INTO edge (from_node, to_node, cluster)
                    VALUES (%(from_node)s, %(to_node)s, %(cluster)s)
                """, dict(from_node=edge.from_node.id, to_node=edge.to_node.id, cluster=cluster_id))
            return task_graph


class Graph:
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
        pending = len(self.nodes_for_state(State.PENDING))
        failed = len(self.nodes_for_state(State.FAILED))
        complete = len(self.nodes_for_state(State.COMPLETE))
        return "{:.2f}% done:  Pending: {}, Failed: {}, Complete: {}".format(self.percent_complete(), pending, failed, complete)

    def percent_complete(self):
        return len(self.nodes_for_state(State.COMPLETE)) * 100 / len(self.graph)

    def draw(self, path):
        agraph = nx.nx_agraph.to_agraph(self.graph)
        agraph.layout('dot', args='-Nfontsize=10 -Nwidth=".2" -Nheight=".2" -Nmargin=0 -Gfontsize=8')
        agraph.draw(path)

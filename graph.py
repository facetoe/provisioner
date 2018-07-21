from abc import abstractmethod
from typing import Tuple

import networkx as nx

from tasks import *


class Graph:
    def __init__(self, num_nodes, num_dcs):
        self.graph, self.root = self.build(num_nodes, num_dcs)

    @abstractmethod
    def build(self, num_nodes: int, num_dcs: int) -> Tuple[nx.DiGraph, Task]:
        pass

    def runnable_tasks(self):
        return self._gather_runnable(self.root)

    def _gather_runnable(self, root):
        runnable = []
        if root.runnable:
            runnable.append(root)
        for node in self.graph.successors(root):
            if node.runnable:
                runnable.append(node)
            runnable.extend(self._gather_runnable(node))
        return set(runnable)

    def provisioned(self):
        for node in self.graph.nodes():
            if node.state != State.COMPLETE:
                return False
        return True

    def percent_complete(self):
        return sum((1 for n in self.graph.nodes() if n.state == State.COMPLETE)) * 100 / len(self.graph)

    def draw(self, path):
        agraph = nx.nx_agraph.to_agraph(self.graph)
        agraph.layout('dot', args='-Nfontsize=10 -Nwidth=".2" -Nheight=".2" -Nmargin=0 -Gfontsize=8')
        agraph.draw(path)


class AWSGraph(Graph):
    def build(self, num_nodes: int, num_dcs: int) -> Tuple[nx.DiGraph, Task]:
        task_graph = nx.DiGraph()

        cluster = Cluster(task_graph)
        for _ in range(num_dcs):
            dc = DataCentre(task_graph)
            role = Role(task_graph)
            vpc = VPC(task_graph)
            security_groups = SecurityGroups(task_graph)
            internet_gateway = InternetGateway(task_graph)
            route_table = RouteTable(task_graph)
            subnets = SubNets(task_graph)
            firewall_rules = FirewallRules(task_graph)

            cluster_edges = [
                (cluster, dc),
                (dc, role),
                (dc, vpc),
                (vpc, security_groups),
                (vpc, internet_gateway),
                (vpc, route_table),
                (internet_gateway, route_table),
                (vpc, subnets),
                (vpc, security_groups),
                (security_groups, firewall_rules),
            ]

            for n in range(num_nodes):
                create_instance = CreateInstance(task_graph)
                create_ebs = CreateEBS(task_graph)
                attach_ebs = AttachEBS(task_graph)

                cluster_edges.append((dc, create_instance))
                cluster_edges.append((dc, create_ebs))

                cluster_edges.append((create_ebs, attach_ebs))
                cluster_edges.append((create_instance, attach_ebs))
                cluster_edges.append((create_instance, BindIP(task_graph)))

                bind_security_group = BindSecurityGroup(task_graph)
                cluster_edges.append((create_instance, bind_security_group))
                cluster_edges.append((security_groups, bind_security_group))

            task_graph.add_edges_from(cluster_edges)
        return task_graph, cluster

import json
from abc import abstractmethod
from collections import namedtuple
from enum import Enum

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


ParentPayload = namedtuple('ParentPayload', 'type state payload')


class Task:
    def __init__(self, graph, task_id=None, task_state=None, payload=None):
        self.graph = graph
        self.id = task_id
        self._state = TaskState(task_state) if task_state else TaskState.PENDING_PROVISION
        self.payload = payload

    @abstractmethod
    def provision(self, cursor, provider):
        # if randint(0, 100) % 10 == 0:
        #     raise Exception("I FAILED")
        # time.sleep(randint(0, 1))
        # self.set_state(cursor, TaskState.PROVISIONED)
        pass

    @abstractmethod
    def delete(self, cursor, provider):
        pass

    def __call__(self, cursor, provider, action):
        try:
            if action == TaskAction.PROVISION:
                self.set_state(cursor, TaskState.PROVISIONING)
                if self.provision(cursor, provider):
                    self.set_state(cursor, TaskState.PROVISIONED)
            elif action == TaskAction.DELETE:
                self.set_state(cursor, TaskState.DELETING)
                self.delete(cursor, provider)
            else:
                raise Exception("Invalid task action: {}".format(action))
            return self
        except Exception as e:
            self.set_state(cursor, TaskState.FAILED)
            raise ExecutionException(self, e)

    def persist(self, cursor, cluster_id, data_centre_id=None):
        if self.id is not None:
            raise Exception("This task is already persisted!")
        cursor.execute("""
            INSERT INTO node (type, payload, cluster, data_centre)
            VALUES (%(type)s, %(payload)s, %(cluster)s, %(data_centre)s) 
            RETURNING id
        """, dict(type=type(self).__name__,
                  payload=json.dumps({}),
                  cluster=cluster_id,
                  data_centre=data_centre_id))
        self.id = cursor.fetchone().id
        return self

    def set_state(self, cursor, state):
        cursor.execute("""
            UPDATE node
            SET state = %(state)s
            WHERE id = %(id)s
        """, dict(state=state.name, id=self.id))
        self._state = state

    def set_payload(self, cursor, payload):
        cursor.execute("""
            UPDATE node
            SET payload = %(payload)s
            WHERE id = %(id)s
        """, dict(payload=json.dumps(payload), id=self.id))
        self.payload = payload

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

    def retry_failed_provision(self):
        self._state = TaskState.PENDING_PROVISION

    @property
    def state(self):
        return self._state

    def _get_parents(self, cursor):
        cursor.execute("""
            SELECT node.type, node.state, node.payload
            FROM edge
                INNER JOIN node
                ON node.id = edge.from_node
            WHERE edge.to_node = %(id)s
        """, dict(id=self.id))
        return [ParentPayload(*r) for r in cursor]

    def _get_parent(self, cursor, task_type):
        cursor.execute("""
            SELECT node.type, node.state, node.payload
            FROM edge
                INNER JOIN node
                ON node.id = edge.from_node
            WHERE edge.to_node = %(id)s
            AND node.type = %(type)s
        """, dict(id=self.id, type=task_type))
        return ParentPayload(*cursor.fetchone())

    def __str__(self):
        return "{}-{}".format(type(self).__name__, id(self))

    def __repr__(self):
        return "{}(id='{}', state='{}')".format(type(self).__name__, self.id, self.state.name)


class DataCentre(Task):
    def provision(self, cursor, provider):
        self.set_state(cursor, TaskState.PROVISIONED)

    def delete(self, cursor, provider):
        self.set_state(cursor, TaskState.DELETED)


class Cluster(Task):
    def provision(self, cursor, provider):
        self.set_state(cursor, TaskState.PROVISIONED)

    def delete(self, cursor, provider):
        self.set_state(cursor, TaskState.DELETED)


class Role(Task):
    pass


class VPC(Task):
    def provision(self, cursor, provider):
        vpc = provider.create_vpc(CidrBlock='192.168.0.0/16')
        vpc.create_tags(Tags=[{"Key": "Name", "Value": "my new vpc bro"}])
        vpc.wait_until_available()
        self.set_payload(cursor, dict(vpc_id=vpc.id))
        return True


class BindSecurityGroup(Task):
    pass


class InternetGateway(Task):
    def provision(self, cursor, provider):
        parent = self._get_parent(cursor, 'VPC')
        vpc_id = parent.payload['vpc_id']
        gateway = provider.create_internet_gateway()
        gateway.create_tags(Tags=[{"Key": "Name", "Value": " Gateway for me brah"}])

        vpc = provider.Vpc(vpc_id)
        vpc.attach_internet_gateway(
            InternetGatewayId=gateway.id,
            VpcId=vpc_id
        )
        self.set_payload(cursor, dict(gateway_id=gateway.id, vpc_id=vpc_id))
        return True


class RouteTable(Task):

    def provision(self, cursor, provider):
        parent = self._get_parent(cursor, 'InternetGateway')
        payload = parent.payload
        vpc = provider.Vpc(payload['vpc_id'])

        route_table = vpc.create_route_table()
        route_table.create_tags(Tags=[{"Key": "Name", "Value": "yisss, route table"}])
        payload['route_table_id'] = route_table.id

        route_table.create_route(
            DestinationCidrBlock='0.0.0.0/0',
            GatewayId=payload['gateway_id'])

        self.set_payload(cursor, payload)
        return True


class SubNets(Task):
    def provision(self, cursor, provider):
        parent = self._get_parent(cursor, 'RouteTable')
        payload = parent.payload
        subnet = provider.create_subnet(CidrBlock='192.168.1.0/24', VpcId=payload['vpc_id'])
        subnet.create_tags(Tags=[{"Key": "Name", "Value": "mah subnet"}])
        payload['subnet_id'] = subnet.id

        route_table = provider.RouteTable(payload['route_table_id'])
        route_table.associate_with_subnet(SubnetId=subnet.id)

        self.set_payload(cursor, payload)
        return True


class SecurityGroups(Task):
    def provision(self, cursor, provider):
        parent = self._get_parent(cursor, 'VPC')
        payload = parent.payload

        vpc = provider.Vpc(payload['vpc_id'])
        sec_group = provider.create_security_group(
            GroupName='slice_0', Description='slice_0 sec group', VpcId=vpc.id)
        sec_group.create_tags(Tags=[{"Key": "Name", "Value": "mah security group"}])

        sec_group.authorize_ingress(
            CidrIp='0.0.0.0/0',
            IpProtocol='icmp',
            FromPort=-1,
            ToPort=-1
        )

        self.set_payload(cursor, dict(security_group_id=sec_group.id))
        return True


class FirewallRules(Task):
    def provision(self, cursor, provider):
        self.set_state(cursor, TaskState.PROVISIONED)


class CreateEBS(Task):
    def provision(self, cursor, provider):
        self.set_state(cursor, TaskState.PROVISIONED)


class AttachEBS(Task):
    def provision(self, cursor, provider):
        self.set_state(cursor, TaskState.PROVISIONED)


class CreateInstance(Task):
    def provision(self, cursor, provider):
        # sub_net_parent = self._get_parent(cursor, 'SubNets')
        # subnet_id = sub_net_parent.payload['subnet_id']
        # sec_group_parent = self._get_parent(cursor, 'SecurityGroups')
        # sec_group_id = sec_group_parent.payload['security_group_id']

        instances = provider.create_instances(
            ImageId='ami-25a8db1f', InstanceType='t2.micro', MaxCount=1, MinCount=1,
            NetworkInterfaces=[{'SubnetId': 'subnet-52152035', 'DeviceIndex': 0, 'AssociatePublicIpAddress': True,
                                'Groups': ['sg-6ff64317']}])
        instances[0].wait_until_running()
        self.set_payload(cursor, dict(instance_id=instances[0].id))
        self.set_state(cursor, TaskState.PROVISIONED)


class BindIP(Task):
    pass

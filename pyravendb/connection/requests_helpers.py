from pyravendb.custom_exceptions.exceptions import InvalidOperationException
from threading import Lock, Timer


class ServerNode(object):
    def __init__(self, url, database, cluster_tag=None):
        self.url = url
        self.database = database
        self.cluster_tag = cluster_tag
        self._response_time = []
        self._is_rate_surpassed = None

    @property
    def response_time(self):
        return self._response_time

    @response_time.setter
    def response_time(self, value):
        self._response_time.insert(len(self._response_time) % 5, value)

    def ewma(self):
        ewma = 0
        divide = len(self._response_time)
        if len(self._response_time) == 0:
            return ewma
        for time in self._response_time:
            ewma += time
        return ewma / divide if divide != 0 else 0

    def is_rate_surpassed(self, request_time_sla_threshold_in_milliseconds):
        if self._is_rate_surpassed:
            self._is_rate_surpassed = self.ewma() >= 0.75 * request_time_sla_threshold_in_milliseconds
        else:
            self._is_rate_surpassed = self.ewma() >= request_time_sla_threshold_in_milliseconds

        return self._is_rate_surpassed


class Topology(object):
    def __init__(self, etag=0, nodes=None):
        self.etag = etag
        self.nodes = nodes

    @staticmethod
    def convert_json_topology_to_entity(json_topology):
        topology = Topology()
        topology.etag = json_topology["Etag"]
        topology.nodes = []
        for node in json_topology["Nodes"]:
            topology.nodes.append(ServerNode(node['Url'], node['Database'], node['ClusterTag']))
        return topology


class NodeSelector(object):
    def __init__(self, topology):
        self.topology = topology
        self._current_node_index = 0
        self.lock = Lock()

    @property
    def current_node_index(self):
        return self._current_node_index

    def on_failed_request(self, node_index):
        topology_nodes_len = len(self.topology.nodes)
        if topology_nodes_len == 0:
            raise InvalidOperationException("Empty database topology, this shouldn't happen.")

        if node_index < topology_nodes_len - 1:
            next_node_index = node_index + 1
        else:
            next_node_index = 0

        self._current_node_index = next_node_index

    def on_update_topology(self, topology, force_update=False):
        if topology is None:
            return False

        old_topology = self.topology
        while True:
            if old_topology.etag >= topology.etag and not force_update:
                return False

            with self.lock:
                if not force_update:
                    self._current_node_index = 0

                if old_topology is self.topology:
                    self.topology = topology
                    return True

            old_topology = self.topology

    def get_current_node(self):
        if len(self.topology.nodes) == 0:
            raise InvalidOperationException("Empty database topology, this shouldn't happen.")

        return self.topology.nodes[self._current_node_index]


class NodeStatus(object):
    def __init__(self, request_executor, node_index, node):
        self._request_executor = request_executor
        self.node_index = node_index
        self.node = node
        self._timer_period = 0
        self._timer = None

    def _next_timer_period(self):
        if not self._timer_period >= 60 * 5:
            self._timer_period += 0.1

        return 60 * 5 if self._timer_period >= 60 * 5 else self._timer_period

    def start_timer(self):
        self._timer = Timer(self._next_timer_period, self._request_executor.check_node_status, args=(self,))

    def __del__(self):
        if self._timer is not None:
            self._timer.cancel()

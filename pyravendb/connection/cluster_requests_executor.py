from pyravendb.commands.raven_commands import GetClusterTopologyCommand, GetTcpInfoCommand
from pyravendb.connection.requests_executor import RequestsExecutor
from pyravendb.connection.requests_helpers import *
from pyravendb.custom_exceptions import exceptions
import hashlib
import json
import os

import logging

logging.basicConfig(filename='cluster_requests_executor_info.log', level=logging.DEBUG)
log = logging.getLogger()

TOPOLOGY_FILES_DIR = os.path.join(os.getcwd(), "topology_files")


class ClusterRequestExecutor(RequestsExecutor):
    def __new__(cls, *args, **kwargs):
        return super(ClusterRequestExecutor, cls).__new__(cls)

    def __init__(self, certificate, convention=None, **kwargs):
        super(ClusterRequestExecutor, self).__init__(None, certificate, convention, **kwargs)
        self.update_cluster_topology_lock = Lock()

    @staticmethod
    def create(urls, certificate, convention=None, **kwargs):
        executor = ClusterRequestExecutor(certificate, convention, **kwargs)
        executor.start_first_topology_thread(urls)
        return executor

    @staticmethod
    def create_for_single_node(url, certificate, convention=None):
        topology = Topology(etag=-1, nodes=[ServerNode(url)])
        return ClusterRequestExecutor(certificate, convention, node_selector=NodeSelector(topology), topology_etag=-2,
                                      disable_topology_updates=True)

    def update_topology(self, node, force_update=False):
        if self._closed:
            return

        if self.update_cluster_topology_lock.acquire(False):
            try:
                if self._closed:
                    return False

                command = GetClusterTopologyCommand()
                response = self.execute_with_node(node, command, should_retry=False)

                hash_name = hashlib.md5(
                    "{0}".format(node.url).encode(
                        'utf-8')).hexdigest()

                topology_file = os.path.join(TOPOLOGY_FILES_DIR, hash_name + ".raven-cluster-topology")
                try:
                    with open(topology_file, 'w') as outfile:
                        json.dump(response, outfile, ensure_ascii=False)
                except:
                    pass

                topology = Topology()
                for key, url in response["Topology"]["Members"].items():
                    topology.nodes.append(ServerNode(url, cluster_tag=key))
                if self._node_selector is None:
                    self._node_selector = NodeSelector(topology)

                elif self._node_selector.on_update_topology(topology, force_update):
                    self.cancel_all_failed_nodes_timers()

                self.topology_etag = self._node_selector.topology.etag
            finally:
                self.update_cluster_topology_lock.release()
        else:
            return False

    def raise_exceptions(self, error_list):
        raise exceptions.AggregateException("Failed to retrieve cluster topology from all known nodes", error_list)

    def try_load_from_cache(self, url):
        server_hash = hashlib.md5(
            "{0}{1}".format(url, self._database_name).encode(
                'utf-8')).hexdigest()
        cluster_topology_file_path = os.path.join(TOPOLOGY_FILES_DIR, server_hash + ".raven-cluster-topology")
        try:
            with open(cluster_topology_file_path, 'r') as cluster_topology_file:
                json_file = json.load(cluster_topology_file)
                topology = Topology()
                for key, url in json_file["Topology"]["Members"].items():
                    topology.nodes.append(ServerNode(url, cluster_tag=key))
                self._node_selector = NodeSelector(
                    Topology.convert_json_topology_to_entity(json.load(topology)))
                return True
        except (FileNotFoundError, json.JSONDecodeError) as e:
            log.info(e)
        return False

    def perform_health_check(self, node, node_status):
        command = GetTcpInfoCommand(tag="health-check")
        try:
            self.execute_with_node(node, command, should_retry=False)

        except Exception as e:
            log.info("{0} is still down".format(node.cluster_tag), e)
            failed_node_timer = self._failed_nodes_timers.get(node_status.node, None)
            if failed_node_timer is not None:
                failed_node_timer.start_timer()
            return

        failed_node_timer = self._failed_nodes_timers.pop(node_status.node, None)
        if failed_node_timer:
            del failed_node_timer

        self._node_selector.restore_node_index(node_status.node_index)

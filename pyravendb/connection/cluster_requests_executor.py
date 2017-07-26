from pyravendb.d_commands.raven_commands import GetClusterTopologyCommand
from pyravendb.connection.requests_executor import RequestsExecutor
from pyravendb.connection.requests_helpers import *
import hashlib
import json
import os

import logging

logging.basicConfig(filename='cluster_requests_executor_info.log', level=logging.DEBUG)
log = logging.getLogger()


class ClusterRequestExecutor(RequestsExecutor):
    def __init__(self, api_key, convention, **kwargs):
        super(ClusterRequestExecutor, self).__init__(None, api_key, convention, **kwargs)
        self.update_cluster_topology_lock = Lock()

    @staticmethod
    def create(urls, api_key, convention, **kwargs):
        executor = ClusterRequestExecutor(api_key, convention, **kwargs)
        executor.start_first_topology_thread(urls)
        return executor

    @staticmethod
    def create_for_single_node(url, api_key, convention):
        topology = Topology(etag=-1, nodes=[ServerNode(url)])
        return ClusterRequestExecutor(api_key, convention, node_selector=NodeSelector(topology), topology_etag=-2,
                                      disable_topology_updates=True)

    def update_topology(self, node):
        if self.update_cluster_topology_lock.acquire(False):
            try:
                command = GetClusterTopologyCommand()
                response = self.execute_with_node(node, command, should_retry=False)

                hash_name = hashlib.md5(
                    "{0}".format(node.url).encode(
                        'utf-8')).hexdigest()

                topology_file = "{0}\{1}.raven-cluster-topology".format(os.getcwd(), hash_name)
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

                elif self._node_selector.on_update_topology(topology):
                    self.cancel_all_failed_nodes_timers()

                self.topology_etag = self._node_selector.topology.etag
            except:
                pass
            finally:
                self.update_cluster_topology_lock.release()
        else:
            return False

    def try_load_from_cache(self, url):
        server_hash = hashlib.md5(
            "{0}{1}".format(url, self._database_name).encode(
                'utf-8')).hexdigest()
        cluster_topology_file_path = "{0}\{1}.raven-cluster-topology".format(os.getcwd(), server_hash)
        try:
            with open(cluster_topology_file_path, 'r') as cluster_topology_file:
                json_file = json.load(cluster_topology_file)
                topology = Topology()
                for key, url in json_file["Topology"]["Members"].items():
                    topology.nodes.append(ServerNode(url, cluster_tag=key))
                self._node_selector = NodeSelector(
                    Topology.convert_json_topology_to_entity(json.load(topology)))
                self.topology_etag = -2
                # TODO set topology Timer
                return True
        except (FileNotFoundError, json.JSONDecodeError) as e:
            log.info(e)
        return False

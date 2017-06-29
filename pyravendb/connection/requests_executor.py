from pyravendb.d_commands.raven_commands import GetTopologyCommand, GetStatisticsCommand
from pyravendb.connection.requests_helpers import NodeSelector, Topology, ServerNode, NodeStatus
from pyravendb.custom_exceptions import exceptions
from pyravendb.tools.authenticator import ApiKeyAuthenticator
from pyravendb.data.document_convention import DocumentConvention
from threading import Timer, Lock, Thread
from datetime import datetime
import time
import requests
import sys
import json
import hashlib
import os

import logging

logging.basicConfig(filename='info.log', level=logging.DEBUG)
log = logging.getLogger()


class RequestsExecutor(object):
    def __init__(self, database_name, api_key, **kwargs):
        self._database_name = database_name
        self._api_key = api_key
        self.topology_etag = kwargs.get("topology_etag", 0)
        self._last_return_response = datetime.utcnow()
        self.convention = None
        self._node_selector = kwargs.get("node_selector", None)
        self._last_known_urls = []

        self.headers = {"Accept": "application/json",
                        "Has-Api-key": 'true' if self._api_key is not None else 'false',
                        "Raven-Client-Version": "4.0.0.0"}

        self.update_topology_lock = Lock()
        self.update_timer_lock = Lock()
        self.lock = Lock()
        self._without_topology = kwargs.get("without_topology", False)

        self._failed_nodes_timers = {}
        self._first_topology_update = None
        self._authenticator = ApiKeyAuthenticator()
        self.cluster_token = None

    @classmethod
    def create(cls, urls, database_name, api_key, convention=None):
        executor = cls(database_name, api_key)
        executor.convention = convention if convention is not None else DocumentConvention()
        executor._first_topology_update = Thread(target=cls.first_topology_update, args=(urls,))
        return executor

    @classmethod
    def create_for_single_node(cls, url, database_name, api_key):
        topology = Topology(etag=-1, nodes=[ServerNode(url, database_name)])
        return cls(database_name, api_key, node_selector=NodeSelector(topology), topology_etag=-2,
                   without_topology=True)

    def execute(self, raven_command, should_retry=True):
        if not hasattr(raven_command, 'raven_command'):
            raise ValueError("Not a valid command")

        try:
            topology_update = self._first_topology_update
            if topology_update is None:
                with self.lock:
                    if self._first_topology_update is None:
                        if self._last_known_urls is None:
                            raise exceptions.InvalidOperationException(
                                "No known topology and no previously known one, cannot proceed, likely a bug")
                        self._first_topology_update = self.first_topology_update(self._last_known_urls)
                    topology_update = self._first_topology_update
                topology_update.join()
        except:
            with self.lock:
                if self._first_topology_update == topology_update:
                    self._first_topology_update = None
            raise

        chosen_node = self._node_selector.get_current_node()

        while True:
            raven_command.create_request(chosen_node)
            if self.cluster_token is not None:
                raven_command.headers["Raven-Authorization", self.cluster_token]
            node_index = self._node_selector.current_node_index

            with requests.session() as session:
                raven_command.headers.update(self.headers)
                if not self._without_topology:
                    raven_command.headers["Topology-Etag"] = "\"{0}\"".format(self.topology_etag)

                data = None if raven_command.data is None else \
                    json.dumps(raven_command.data, default=self.convention.json_default_method)
                start_time = time.time() * 1000
                end_time = None
                try:
                    if self.cluster_token is not None:
                        raven_command.headers["Raven-Authorization"] = chosen_node.current_token
                    response = session.request(raven_command.method, url=raven_command.url, data=data,
                                               headers=raven_command.headers)
                except exceptions.ErrorResponseException as e:
                    end_time = time.time() * 1000
                    if not should_retry:
                        raise
                    if not self.handle_server_down(chosen_node, node_index, raven_command, response, e):
                        raise exceptions.AllTopologyNodesDownException(
                            "Tried to send request to all configured nodes in the topology, "
                            "all of them seem to be down or not responding.", e)

                    raven_command.failed_nodes.add(chosen_node)
                    chosen_node = self.choose_node_for_request(raven_command)
                    continue

                finally:
                    if not end_time:
                        end_time = time.time() * 1000
                    elapsed_time = end_time - start_time
                    chosen_node.response_time = elapsed_time

                if response is None:
                    raise ValueError("response is invalid.")

                if response.status_code == 404:
                    return raven_command.set_response(None)
                if response.status_code == 412 or response.status_code == 401:
                    if not self._api_key:
                        raise exceptions.AuthorizationException(
                            "Got unauthorized response for {0}. Please specify an api-key.".format(
                                chosen_node.url))
                    raven_command.authentication_retries += 1
                    if raven_command.authentication_retries > 1:
                        raise exceptions.AuthorizationException(
                            "Got unauthorized response for {0} after trying to authenticate using specified api-key.".format(
                                chosen_node.url))

                    self.handle_unauthorized(chosen_node)
                    continue
                if response.status_code == 403:
                    raise exceptions.AuthorizationException(
                        "Forbidden access to {0}. Make sure you're using the correct api-key.".format(
                            chosen_node.url))
                if response.status_code == 408 or response.status_code == 502 or response.status_code == 503 or response.status_code == 504:
                    self.handle_server_down(chosen_node, node_index, raven_command, response, None)
                    chosen_node = self.choose_node_for_request(raven_command)
                    continue
                if response.status_code == 409:
                    # TODO: Conflict resolution
                    # current implementation is temporary
                    raise NotImplementedError(response.status_code)

                if "Refresh-Topology" in response.headers:
                    self.update_topology(ServerNode(chosen_node.url, self._database_name))
                self._last_return_response = datetime.utcnow()
                return raven_command.set_response(response)

    def first_topology_update(self, initial_urls):
        list = []
        for url in initial_urls:
            try:
                self.update_topology(ServerNode(url, self._database_name))
            except:
                e = sys.exc_info()[0]
                if len(initial_urls) == 0:
                    raise exceptions.InvalidOperationException("Cannot get topology from server: " + url, e)
                list.append((url, e))

    def update_topology(self, node, timeout):
        if self.update_topology_lock(False):
            with self.update_topology_lock:
                command = GetTopologyCommand()
                response = self.execute(node, command, should_retry=False)

                hash_name = hashlib.md5(
                    "{0}{1}".format(node.url, node.database).encode(
                        'utf-8')).hexdigest()

                topology_file = "{0}\{1}.raven-topology".format(os.getcwd(), hash_name)
                try:
                    with open(topology_file, 'w') as outfile:
                        json.dump(response, outfile, ensure_ascii=False)
                except:
                    pass

                if self._node_selector is None:
                    self._node_selector = NodeSelector(response)

                elif self._node_selector.on_update_topology(response):
                    self.cancel_all_failed_nodes_timers()

                self.topology_etag = self._node_selector.topology.etag

        else:
            return False

    def handle_server_down(self, chosen_node, node_index, command, response, e):
        command.failed_nodes.update({chosen_node: {"url": command.url, "error": str(e), "type": type(e).__name__}})
        node_selector = self._node_selector

        node_status = NodeStatus(self, node_index, chosen_node)
        with self.update_timer_lock:
            self._failed_nodes_timers.update({chosen_node, node_status})
            node_status.start_timer()

        if node_selector is not None:
            node_selector.on_failed_request(node_index)
            current_node = node_selector.get_current_node()
            if command.is_failed_with_node(current_node):
                return True
        return False

    def cancel_all_failed_nodes_timers(self):
        failed_nodes_timers = self._failed_nodes_timers
        self._failed_nodes_timers.clear()
        for _, timer in failed_nodes_timers.items():
            timer.cancel()
            timer.join()

    def check_node_status(self, node_status):
        nodes = self._node_selector.topology.nodes
        server_node = nodes[node_status.node_index]
        if not (node_status.node_index >= len(nodes) or server_node is not node_status.node):
            self.perform_health_check(server_node, node_status)

    def perform_health_check(self, node, node_status):
        command = GetStatisticsCommand(debug_tag="failure=check")
        command.create_request(node)
        try:
            with requests.session() as session:
                response = session.request("GET", command.url, headers=self.headers)
                if response.status_code == 412 or response.status_code == 401:
                    try:
                        self.handle_unauthorized(node)
                    except Exception as e:
                        log.info("{0} is still down".format(node.cluster_tag), e)
                        failed_node_timer = self._failed_nodes_timers.get(node_status.node, None)
                        if failed_node_timer is not None:
                            failed_node_timer.start_timer()
                        pass
                if response.status_code == 200:
                    failed_node_timer = self._failed_nodes_timers.pop(node_status.node, None)
                    if failed_node_timer:
                        del failed_node_timer
                    # TODO restore node_index
        except exceptions.ErrorResponseException:
            pass

    def update_current_token(self):
        if self._unauthorized_timer_initialize:
            topology = self._topology
            leader_node = topology.leader_node

            if leader_node is not None:
                self.handle_unauthorized(leader_node, False)

            for node in topology.nodes:
                self.handle_unauthorized(node, False)
        else:
            self._unauthorized_timer_initialize = True

        update_current_token_timer = Timer(60 * 20, self.update_current_token)
        update_current_token_timer.daemon = True
        update_current_token_timer.start()

    def convert_json_topology_to_entity(self, json_topology):
        topology = Topology()
        topology.etag = json_topology["Etag"]
        topology.nodes = []
        # TODO make sure we make a new serverNode list
        for node in json_topology["Nodes"]:
            topology.nodes.append("")
        return topology

    def handle_unauthorized(self, server_node, should_raise=True):
        try:
            cluster_token = self._authenticator.authenticate(server_node.url, self._api_key, self.headers)
            self.cluster_token = cluster_token
        except Exception as e:
            log.info("Failed to authorize using api key", exc_info=str(e))

            if should_raise:
                raise

        if not self._unauthorized_timer_initialize:
            self.update_current_token()

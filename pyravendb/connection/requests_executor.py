from requests_pkcs12 import Pkcs12Adapter

from pyravendb.commands.raven_commands import GetTopologyCommand, GetStatisticsCommand
from pyravendb.connection.requests_helpers import *
from pyravendb.custom_exceptions import exceptions
from OpenSSL import crypto
from pyravendb.data.document_conventions import DocumentConventions
from threading import Lock
from pyravendb.tools.utils import Utils
from datetime import datetime, timedelta
import time
import requests
import json
import hashlib
import errno
import os
import io
import logging

logging.basicConfig(filename='requests_executor_info.log', level=logging.DEBUG)
log = logging.getLogger()

TOPOLOGY_FILES_DIR = os.path.join(os.getcwd(), "topology_files")


class RequestsExecutor(object):
    def __new__(cls, *args, **kwargs):
        instance = super(RequestsExecutor, cls).__new__(cls)
        try:
            os.makedirs(TOPOLOGY_FILES_DIR)
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise
        return instance

    @staticmethod
    def initialize_certificate(certificate):
        if not isinstance(certificate, dict):
            return certificate, None
        pfx = certificate["pfx"]
        password = certificate.get("password", None)
        adapter = (
            Pkcs12Adapter(pkcs12_filename=pfx, pkcs12_password=password)
            if os.path.isfile(pfx)
            else Pkcs12Adapter(pkcs12_data=pfx, pkcs12_password=password)
        )
        return None, adapter

    def __init__(self, database_name, certificate, conventions=None, **kwargs):
        self._database_name = database_name
        (self._certificate, self._adapter) = self.initialize_certificate(certificate)
        self.topology_etag = kwargs.get("topology_etag", 0)
        self._last_return_response = datetime.utcnow()
        self.conventions = conventions if conventions is not None else DocumentConventions()
        self._node_selector = kwargs.get("node_selector", None)
        self._last_known_urls = None

        self.headers = {"Accept": "application/json",
                        "Raven-Client-Version": "5.0.0.4"}

        self.update_topology_lock = Lock()
        self.update_timer_lock = Lock()
        self.lock = Lock()
        self._disable_topology_updates = kwargs.get("disable_topology_updates", False)

        self._failed_nodes_timers = {}
        self.update_topology_timer = None
        self._first_topology_update = None
        self.cluster_token = None
        self.topology_nodes = None

        self._closed = False

    @property
    def certificate(self):
        return self._certificate

    @staticmethod
    def create(urls, database_name, certificate, conventions=None):
        executor = RequestsExecutor(database_name, certificate, conventions)
        executor.start_first_topology_thread(urls)
        return executor

    @staticmethod
    def create_for_single_node(url, database_name, certificate):
        topology = Topology(etag=-1, nodes=[ServerNode(url, database_name)])
        return RequestsExecutor(database_name, certificate, node_selector=NodeSelector(topology), topology_etag=-2,
                                disable_topology_updates=True)

    def start_first_topology_thread(self, urls):
        self._first_topology_update = PropagatingThread(target=self.first_topology_update, args=(urls,), daemon=True)
        self._first_topology_update.start()

    def ensure_node_selector(self):
        if self._first_topology_update and self._first_topology_update.is_alive():
            self._first_topology_update.join()

        if not self._node_selector:
            self._node_selector = NodeSelector(Topology(etag=self.topology_etag, nodes=self.topology_nodes))

    def get_preferred_node(self):
        self.ensure_node_selector()
        return self._node_selector.get_current_node()

    def execute(self, raven_command, should_retry=True):
        if not hasattr(raven_command, 'raven_command'):
            raise ValueError("Not a valid command")
        topology_update = self._first_topology_update
        if not self._disable_topology_updates:
            if topology_update is None or topology_update.is_alive():
                try:
                    if topology_update is None:
                        with self.lock:
                            if self._first_topology_update is None:
                                if self._last_known_urls is None:
                                    raise exceptions.InvalidOperationException(
                                        "No known topology and no previously known one, cannot proceed, likely a bug")
                                self.start_first_topology_thread(self._last_known_urls)
                            topology_update = self._first_topology_update
                    topology_update.join()
                except Exception as e:
                    log.debug(str(e))
                    with self.lock:
                        if self._first_topology_update == topology_update:
                            self._first_topology_update = None
                    raise

        if self._node_selector is None:
            raise exceptions.InvalidOperationException("A connection with the server could not be established",
                                                       "node_selector cannot be None, please check your connection "
                                                       "or supply a valid node_selector")
        chosen_node = self._node_selector.get_current_node()
        return self.execute_with_node(chosen_node, raven_command, should_retry)

    def execute_with_node(self, chosen_node, raven_command, should_retry):
        while True:
            raven_command.create_request(chosen_node)
            node_index = 0 if self._node_selector is None else self._node_selector.current_node_index

            with requests.session() as session:
                if self._adapter is not None:
                    session.mount("https://", self._adapter)
                if raven_command.is_raft_request:
                    prefix = '&' if '?' in raven_command.url else '?'
                    raven_command.url += f"{prefix}raft-request-id={raven_command.raft_unique_request_id}"
                raven_command.headers.update(self.headers)
                if not self._disable_topology_updates:
                    raven_command.headers["Topology-Etag"] = "\"{0}\"".format(self.topology_etag)

                data = raven_command.data
                if data and not isinstance(data, io.BufferedIOBase):
                    data = json.dumps(raven_command.data, default=self.conventions.json_default_method)

                if raven_command.files:
                    data = {"data": data}

                start_time = time.time() * 1000
                end_time = None
                try:
                    response = session.request(raven_command.method, url=raven_command.url, data=data,
                                               files=raven_command.files,
                                               cert=self._certificate, headers=raven_command.headers,
                                               stream=raven_command.use_stream)
                except Exception as e:
                    end_time = time.time() * 1000
                    if not should_retry:
                        raise
                    if not self.handle_server_down(chosen_node, node_index, raven_command, e):
                        raise exceptions.AllTopologyNodesDownException(
                            "Tried to send request to all configured nodes in the topology, "
                            "all of them seem to be down or not responding.", e)

                    chosen_node = self._node_selector.get_current_node()
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
                if response.status_code == 403:
                    if self._certificate is not None:
                        cert = self._certificate
                        if isinstance(cert, tuple):
                            (cert, _) = cert

                        with open(cert, 'rb') as pem:
                            cert = crypto.load_certificate(crypto.FILETYPE_PEM, pem.read())
                            name = str(cert.get_subject().get_components()[0][1])
                    raise exceptions.AuthorizationException(
                        "Forbidden access to " + chosen_node.database + "@" + chosen_node.url + ", " +
                        ("a certificate is required." if self._certificate is None
                         else name + " does not have permission to access it or is unknown.") +
                        response.request.method + " " + response.request.path_url)
                if response.status_code == 410:
                    if should_retry:
                        self.update_topology(chosen_node, True)
                        # TODO update this after build make fastest node selector
                if response.status_code == 408 or response.status_code == 502 or response.status_code == 503 or response.status_code == 504:
                    if len(raven_command.failed_nodes) == 1:
                        node = list(raven_command.failed_nodes.keys())[0]
                        database_missing = response.headers.get("Database-Missing", None)
                        if database_missing:
                            raise exceptions.DatabaseDoesNotExistException(
                                "Database " + database_missing + " does not exists")

                        raise exceptions.UnsuccessfulRequestException(node.url, raven_command.failed_nodes[node])

                    try:
                        e = response.json()["Message"]
                    except ValueError:
                        e = None
                    if self.handle_server_down(chosen_node, node_index, raven_command, e):
                        chosen_node = self._node_selector.get_current_node()
                    continue
                if response.status_code == 409:
                    # TODO: Conflict resolution
                    # current implementation is temporary
                    try:
                        response = response.json()
                        if "Error" in response:
                            raise Exception(response["Message"], response["Type"])
                    except ValueError:
                        raise response.raise_for_status()

                if "Refresh-Topology" in response.headers:
                    self.update_topology(ServerNode(chosen_node.url, self._database_name))
                self._last_return_response = datetime.utcnow()
                return raven_command.set_response(response)

    def first_topology_update(self, initial_urls):
        error_list = []
        for url in initial_urls:
            try:
                self.update_topology(ServerNode(url, self._database_name))
                self.update_topology_timer = Utils.start_a_timer(60 * 5, self.update_topology_callback, daemon=True)
                self.topology_nodes = self._node_selector.topology.nodes
                return
            except exceptions.DatabaseDoesNotExistException:
                # Will happen on all node in the cluster, so errors immediately
                self._last_known_urls = initial_urls
                raise
            except Exception as e:
                if len(initial_urls) == 0:
                    self._last_known_urls = initial_urls
                    raise exceptions.InvalidOperationException("Cannot get topology from server: " + url, e)
                error_list.append((url, e))

        # Failed to update topology trying update from cache
        for url in initial_urls:
            if self.try_load_from_cache(url):
                self.topology_nodes = self._node_selector.topology.nodes
                return

        self._last_known_urls = initial_urls
        self.raise_exceptions(error_list)

    def raise_exceptions(self, error_list):
        raise exceptions.AggregateException("Failed to retrieve database topology from all known nodes", error_list)

    def try_load_from_cache(self, url):
        server_hash = hashlib.md5(
            "{0}{1}".format(url, self._database_name).encode(
                'utf-8')).hexdigest()
        topology_file_path = os.path.join(TOPOLOGY_FILES_DIR, server_hash + ".raven-topology")
        try:
            with open(topology_file_path, 'r') as topology_file:
                json_file = json.load(topology_file)
                self._node_selector = NodeSelector(
                    Topology.convert_json_topology_to_entity(json_file))
                self.topology_etag = -2
                self.update_topology_timer = Utils.start_a_timer(60 * 5, self.update_topology_callback, daemon=True)
                return True
        except (FileNotFoundError, json.JSONDecodeError) as e:
            log.info(e)
        return False

    def update_topology(self, node, force_update=False):
        if self._closed:
            return

        if self.update_topology_lock.acquire(False):
            try:
                if self._closed:
                    return False

                command = GetTopologyCommand()
                response = self.execute_with_node(node, command, should_retry=False)

                hash_name = hashlib.md5(
                    "{0}{1}".format(node.url, node.database).encode(
                        'utf-8')).hexdigest()

                topology_file = os.path.join(TOPOLOGY_FILES_DIR, hash_name + ".raven-topology")
                try:
                    with open(topology_file, 'w') as outfile:
                        json.dump(response, outfile, ensure_ascii=False)
                except (IOError, json.JSONDecodeError):
                    pass

                topology = Topology.convert_json_topology_to_entity(response)
                if self._node_selector is None:
                    self._node_selector = NodeSelector(topology)

                elif self._node_selector.on_update_topology(topology, force_update):
                    self.cancel_all_failed_nodes_timers()

                self.topology_etag = self._node_selector.topology.etag
            finally:
                self.update_topology_lock.release()
        else:
            return False

    def handle_server_down(self, chosen_node, node_index, command, e):
        command.failed_nodes.update({chosen_node: {"url": command.url, "error": str(e), "type": type(e).__name__}})
        node_selector = self._node_selector

        if node_selector is None:
            return False

        if chosen_node not in self._failed_nodes_timers:
            node_status = NodeStatus(self, node_index, chosen_node)
            with self.update_timer_lock:
                if self._failed_nodes_timers.get(chosen_node, None) is None:
                    self._failed_nodes_timers.update({chosen_node: node_status})
                    node_status.start_timer()

        node_selector.on_failed_request(node_index)
        current_node = node_selector.get_current_node()
        if command.is_failed_with_node(current_node):
            return False
        return True

    def cancel_all_failed_nodes_timers(self):
        failed_nodes_timers = self._failed_nodes_timers
        self._failed_nodes_timers.clear()
        for _, timer in failed_nodes_timers.items():
            timer.cancel()
            timer.join()

    def check_node_status(self, node_status):
        if self._node_selector is not None:
            nodes = self._node_selector.topology.nodes
            if node_status.node_index >= len(nodes):
                return
            server_node = nodes[node_status.node_index]
            if server_node is not node_status.node:
                self.perform_health_check(server_node, node_status)

    def perform_health_check(self, node, node_status):
        command = GetStatisticsCommand(debug_tag="failure=check")
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

    def update_topology_callback(self):
        time = datetime.utcnow()
        if time - self._last_return_response <= timedelta(minutes=5):
            return
        try:
            self.update_topology(self._node_selector.get_current_node())
        except Exception as e:
            log.info("Couldn't Update Topology from _updateTopologyTimer task", e)

    def close(self):
        if self._closed:
            return

        self._closed = True
        self.cancel_all_failed_nodes_timers()
        if self.update_topology_timer:
            self.update_topology_timer.cancel()

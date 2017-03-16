from pyravendb.data.document_convention import ReadBehavior, WriteBehavior
from pyravendb.d_commands.raven_commands import GetTopologyCommand
from Crypto.Cipher import AES, PKCS1_OAEP
from Crypto.PublicKey import RSA
from Crypto.Random import get_random_bytes
from Crypto.Util.number import bytes_to_long
from pyravendb.custom_exceptions import exceptions
from pyravendb.tools.pkcs7 import PKCS7Encoder
from pyravendb.connection.requests_helpers import ServerNode, Topology
from pyravendb.tools.utils import Utils
from threading import Timer, Lock
import logging

logging.basicConfig(filename='info.log', level=logging.DEBUG)
log = logging.getLogger()
import time
import requests
import sys
import json
import hashlib
import base64
import os


class RequestsExecutor(object):
    def __init__(self, url, database, api_key, convention):
        self._api_key = api_key
        server_node = ServerNode(url, database, api_key)
        if sys.version_info.major > 2:
            maxint = sys.maxsize
        else:
            maxint = sys.maxint
        self._topology = Topology(etag=-maxint - 1, leader_node=server_node, read_behavior=ReadBehavior.leader_only,
                                  write_behavior=WriteBehavior.leader_only)

        self.primary = False
        self.first_time_try_load_from_topology_cache = True
        self.version_info = sys.version_info.major
        self.headers = {"Accept": "application/json",
                        "Has-Api-key": 'true' if self._api_key is not None else 'false',
                        "Raven-Client-Version": "4.0.0.0"}
        self.topology_change_counter = 0
        self.lock = Lock()
        self.update_failing_node_status_lock = Lock()
        self.request_count = 0
        self._token = None
        self.convention = convention

        failing_nodes_timer = Timer(60, self.update_failing_nodes_status)
        failing_nodes_timer.daemon = True
        failing_nodes_timer.start()

        self.hash_name = hashlib.md5(
            "{0}{1}".format(self._topology.leader_node.url, self._topology.leader_node.database).encode(
                'utf-8')).hexdigest()
        self.topology_file = "{0}\{1}.raven-topology".format(os.getcwd(), self.hash_name)

        try:
            with open(self.topology_file, 'r') as f:
                replication_topology = json.loads(f.read())
                cache_topology = self.convert_json_topology_to_entity(replication_topology)
                if cache_topology.etag > self._topology.etag:
                    self._topology = cache_topology
        except Exception:
            pass
        finally:
            timer = Timer(1, self.get_replication_topology)
            timer.daemon = True
            timer.start()

    def execute(self, raven_command):
        if not hasattr(raven_command, 'raven_command'):
            raise ValueError("Not a valid command")

        chosen_node = self.choose_node_for_request(raven_command)

        while True:
            raven_command.create_request(chosen_node["node"])

            with requests.session() as session:
                raven_command.headers.update(self.headers)
                data = None if raven_command.data is None else \
                    json.dumps(raven_command.data, default=self.convention.json_default_method)
                start_time = time.time() * 1000
                end_time = None
                try:
                    response = session.request(raven_command.method, url=raven_command.url, data=data,
                                               headers=raven_command.headers)
                except exceptions.ErrorResponseException:
                    end_time = time.time() * 1000
                    chosen_node["node"].is_failed = True
                    raven_command.failed_nodes.add(chosen_node["node"])
                    chosen_node = self.choose_node_for_request(raven_command)
                    continue

                finally:
                    if not end_time:
                        end_time = time.time() * 1000
                    elapsed_time = end_time - start_time
                    chosen_node["node"].response_time = elapsed_time

                if response is None:
                    raise ValueError("response is invalid.")

                if response.status_code == 404:
                    return raven_command.set_response(None)
                if response.status_code == 412 or response.status_code == 401:
                    if not self._api_key:
                        raise exceptions.AuthorizationException(
                            "Got unauthorized response for {0}. Please specify an api-key.".format(
                                chosen_node["node"].url))
                    raven_command.authentication_retries += 1
                    if raven_command.authentication_retries > 1:
                        raise exceptions.AuthorizationException(
                            "Got unauthorized response for {0} after trying to authenticate using specified api-key.".format(
                                chosen_node["node"].url))
                    try:
                        oauth_source = response.headers.__getitem__("OAuth-Source")
                    except KeyError:
                        raise exceptions.InvalidOperationException(
                            "Something is not right please check your server settings (do you use the right api_key)")
                    self.handle_unauthorized(self._api_key, oauth_source)
                    continue
                if response.status_code == 403:
                    raise exceptions.AuthorizationException(
                        "Forbidden access to {0}. Make sure you're using the correct api-key.".format(
                            chosen_node["node"].url))
                if response.status_code == 408 or response.status_code == 502 or response.status_code == 503 or response.status_code == 504:
                    if raven_command.avoid_failover:
                        return raven_command.set_response(None)
                    chosen_node["node"].is_failed = True
                    raven_command.failed_nodes.add(chosen_node["node"])
                    chosen_node = self.choose_node_for_request(raven_command)
                    continue

                return raven_command.set_response(response)

    def is_alive(self, node):
        command = GetTopologyCommand()
        command.create_request(node)
        try:
            with requests.session() as session:
                start_time = time.time() * 1000
                response = session.request("GET", command.url, headers=self.headers)
                if response.status_code == 412 or response.status_code == 401:
                    try:
                        try:
                            oauth_source = response.headers.__getitem__("OAuth-Source")
                        except KeyError:
                            raise exceptions.InvalidOperationException(
                                "Something is not right please check your server settings")
                        self.handle_unauthorized(self._api_key, oauth_source)
                    except Exception as e:
                        log.info("Tested if node alive but it's down: {0}".format(node.url), e)
                        pass
                if response.status_code == 200:
                    node.is_failed = False
        except exceptions.ErrorResponseException:
            pass
        finally:
            elapsed_time = time.time() * 1000 - start_time
            node.response_time = elapsed_time
            if node.is_failed:
                self._open_is_alive_timer(node)

    def choose_node_for_request(self, command):
        topology = self._topology
        leader_node = topology.leader_node

        if command.is_read_request:
            if topology.read_behavior == ReadBehavior.leader_only:
                if not command.is_failed_with_node(leader_node):
                    return {"node": leader_node}
                raise requests.RequestException(
                    "Leader node failed to make this request. the current read_behavior is set to leader_only")

            if topology.read_behavior == ReadBehavior.round_robin:
                skipped_nodes = []
                if topology.nodes:
                    for node in topology.nodes:
                        if not node.is_failed and not command.is_failed_with_node(node):
                            return {"node": node, "skipped_nodes": skipped_nodes}
                        skipped_nodes.append(node)
                raise requests.RequestException(
                    "Tried all nodes in the cluster but failed getting a response")

            if topology.read_behavior == ReadBehavior.leader_with_failover_when_request_time_sla_threshold_is_reached:
                if not leader_node.is_failed and not command.is_failed_with_node(
                        leader_node) and leader_node.is_rate_surpassed(topology.sla):
                    return {"node": leader_node}

                nodes_with_leader = []
                if not leader_node.is_failed:
                    nodes_with_leader = [leader_node]

                if topology.nodes:
                    for node in topology.nodes:
                        if not node.is_failed:
                            nodes_with_leader.append(node)

                if len(nodes_with_leader) > 0:
                    nodes_with_leader = sorted(nodes_with_leader, key=lambda node: node.ewma())
                    fastest_node = nodes_with_leader[0]
                    nodes_with_leader.remove(fastest_node)

                    return {"node": fastest_node, "skipped_nodes": nodes_with_leader}

                raise requests.RequestException("Tried all nodes in the cluster but failed getting a response")

            raise exceptions.InvalidOperationException("Invalid read_behavior value:{0}".format(topology.read_behavior))

        if topology.write_behavior == WriteBehavior.leader_only:
            if not command.is_failed_with_node(leader_node):
                return {"node": leader_node}
            raise requests.RequestException(
                "Leader node failed to make this request. the current write_behavior is set to leader_only")

        if topology.write_behavior == WriteBehavior.leader_with_failover:
            if not leader_node.is_failed and command.is_failed_with_node(leader_node):
                return {"node": leader_node}

            for node in topology.nodes:
                if not node.is_failed and command.is_failed_with_node(node):
                    return {"node": node}

            raise requests.RequestException("Tried all nodes in the cluster but failed getting a response")

        raise exceptions.InvalidOperationException("Invalid write_behavior value: {0}".format(topology.write_behavior))

    def update_failing_nodes_status(self):
        with self.update_failing_node_status_lock:
            try:
                topology = self._topology

                leader_node = topology.leader_node
                if leader_node and leader_node.is_failed:
                    self._open_is_alive_timer(leader_node)

                server_nodes = topology.nodes
                if server_nodes:
                    for node in server_nodes:
                        if node and node.is_failed:
                            self._open_is_alive_timer(node)

            except Exception as e:
                log.info("Failed to check if failing server are down", e)
            finally:
                failing_nodes_timer = Timer(60, self.update_failing_nodes_status)
                failing_nodes_timer.daemon = True
                failing_nodes_timer.start()

    def _open_is_alive_timer(self, node):
        is_alive_timer = Timer(5, lambda: self.is_alive(node))
        is_alive_timer.daemon = True
        is_alive_timer.start()

    def get_replication_topology(self):
        with self.lock:
            try:
                topology_command = GetTopologyCommand()
                response = self.execute(topology_command)
                if response is not None:
                    response_replication_topology = self.convert_json_topology_to_entity(response)
                    if self._topology.etag < response_replication_topology.etag:
                        self._topology = response_replication_topology
                        with open(self.topology_file, 'w+') as f:
                            if "SLA" in response:
                                response["SLA"]["RequestTimeThresholdInMilliseconds"] = float(
                                    response["SLA"]["RequestTimeThresholdInMilliseconds"]) / 1000
                            f.write(json.dumps(response))
            except Exception as e:
                log.info("Failed to update topology", e)
            finally:
                timer = Timer(60 * 5, self.get_replication_topology)
                timer.daemon = True
                timer.start()

    def convert_json_topology_to_entity(self, json_topology):
        topology = Topology()
        topology.etag = json_topology["Etag"]
        topology.leader_node = ServerNode(json_topology["LeaderNode"]["Url"], json_topology["LeaderNode"]["Database"])
        topology.nodes = [ServerNode(node["Url"], node["Database"], node["ApiKey"]) for node in json_topology["Nodes"]]
        topology.read_behavior = ReadBehavior(json_topology["ReadBehavior"])
        topology.write_behavior = WriteBehavior(json_topology["WriteBehavior"])
        topology.sla = float(json_topology["SLA"]["RequestTimeThresholdInMilliseconds"]) / 1000
        return topology

    def handle_unauthorized(self, api_key, oauth_source, second_api_key=None):
        raise NotImplementedError("handle unauthorized requests invalid")
        # api_name, secret = api_key.split('/', 1)
        # tries = 1
        # headers = {"grant_type": "client_credentials"}
        # data = None
        # with requests.session() as session:
        #     while True:
        #         oath = session.request(method="POST", url=oauth_source,
        #                                headers=headers, data=data)
        #         if oath.reason == "Precondition Failed":
        #             if tries > 1:
        #                 if not (second_api_key and self._api_key != second_api_key and tries < 3):
        #                     raise exceptions.ErrorResponseException("Unauthorized")
        #                 api_name, secret = second_api_key.split('/', 1)
        #                 tries += 1
        #
        #             authenticate = oath.headers.__getitem__("www-authenticate")[len("Raven  "):]
        #             challenge_dict = dict(item.split("=", 1) for item in authenticate.split(','))
        #
        #             exponent_str = challenge_dict.get("exponent", None)
        #             modulus_str = challenge_dict.get("modulus", None)
        #             challenge = challenge_dict.get("challenge", None)
        #
        #             exponent = bytes_to_long(base64.standard_b64decode(exponent_str))
        #             modulus = bytes_to_long(base64.standard_b64decode(modulus_str))
        #
        #             rsa = RSA.construct((modulus, exponent))
        #             cipher = PKCS1_OAEP.new(rsa)
        #
        #             iv = get_random_bytes(16)
        #             key = get_random_bytes(32)
        #             encoder = PKCS7Encoder()
        #
        #             cipher_text = cipher.encrypt(key + iv)
        #             results = []
        #             results.extend(cipher_text)
        #
        #             aes = AES.new(key, AES.MODE_CBC, iv)
        #             sub_data = Utils.dict_to_string({"api key name": api_name, "challenge": challenge,
        #                                              "response": base64.b64encode(hashlib.sha1(
        #                                                  '{0};{1}'.format(challenge, secret).encode(
        #                                                      'utf-8')).digest())})
        #
        #             results.extend(aes.encrypt(encoder.encode(sub_data)))
        #             data = Utils.dict_to_string({"exponent": exponent_str, "modulus": modulus_str,
        #                                          "data": base64.standard_b64encode(bytearray(results))})
        #
        #             if exponent is None or modulus is None or challenge is None:
        #                 raise exceptions.InvalidOperationException(
        #                     "Invalid response from server, could not parse raven authentication information:{0} ".format(
        #                         authenticate))
        #             tries += 1
        #         elif oath.status_code == 200:
        #             oath_json = oath.json()
        #             body = oath_json["Body"]
        #             signature = oath_json["Signature"]
        #             if not sys.version_info.major > 2:
        #                 body = body.encode('utf-8')
        #                 signature = signature.encode('utf-8')
        #                 self._token = "Bearer {0}".format(
        #                     {"Body": body, "Signature": signature})
        #                 self.headers.update({"Authorization": self._token})
        #             break
        #         else:
        #             raise exceptions.ErrorResponseException(oath.reason)

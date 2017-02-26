from pyravendb.data.document_convention import DocumentConvention, Failover
from pyravendb.tools.indexqueue import IndexQueue
from Crypto.Cipher import AES, PKCS1_OAEP
from Crypto.PublicKey import RSA
from Crypto.Random import get_random_bytes
from Crypto.Util.number import bytes_to_long
from pyravendb.custom_exceptions import exceptions
from pyravendb.tools.pkcs7 import PKCS7Encoder
from pyravendb.connection.server_node import ServerNode
import tempfile
import requests
import sys
import json
import hashlib
import base64
import os
from pyravendb.tools.utils import Utils
from threading import Timer, Lock


class RequestsExecutor(object):
    def __init__(self, server_node, convention=None, force_get_topology=False):
        self.server_node = server_node
        self._primary_server_node = server_node
        self.primary = False
        self.version_info = sys.version_info.major
        self.convention = convention
        if self.convention is None:
            self.convention = DocumentConvention()
        self.headers = {"Accept": "application/json",
                        "Has-Api-key": 'true' if self.server_node.api_key is not None else 'false',
                        "Raven-Client-Version": "4.0.0.0"}
        self.replication_topology = IndexQueue()
        self.topology_change_counter = 0
        self.lock = Lock()
        self.topology = None
        self.request_count = 0
        self.force_get_topology = force_get_topology
        self._token = None

    def execute(self, raven_command, force_read_from_master=False):
        if not hasattr(raven_command, 'raven_command'):
            raise ValueError("Not a valid command")

        raven_command.create_request(self.server_node)
        if self.force_get_topology:
            self.force_get_topology = False
            self.get_replication_topology()

        if self.server_node is self._primary_server_node and not self.primary:
            self.primary = True

        if self.server_node.database.lower() == self.convention.system_database:
            force_read_from_master = True

        return self._execute(raven_command, force_read_from_master=force_read_from_master)

    def _execute(self, raven_command, force_read_from_master=False):
        while True:
            if not force_read_from_master:
                if raven_command.method == "GET":
                    if (raven_command.url == "replication/topology" or "Hilo" in raven_command.url) \
                            and not self.primary:
                        raise exceptions.InvalidOperationException(
                            "Cant get access to {0} when {1}(primary) is Down".format(raven_command.url,
                                                                                      self._primary_database))
                    elif self.convention.failover_behavior == Failover.read_from_all_servers:
                        with self.lock:
                            self.request_count += 1
                            index = self.request_count % (len(self.replication_topology) + 1)
                        if index != 0 or not self.primary:  # if 0 use the primary
                            index -= 1
                            self.server_node = self.replication_topology.peek(index if index > 0 else 0)
                else:
                    if not self.primary and self.convention.failover_behavior == \
                            Failover.allow_reads_from_secondaries_and_writes_to_secondaries:
                        raise exceptions.InvalidOperationException(
                            "Cant write to server when the primary is down when failover = {0}".format(
                                self.convention.failover_behavior.name))

            if raven_command.url is None:
                if not self.primary:
                    raise exceptions.InvalidOperationException(
                        "Cant read or write to the master because {0} is down".format(
                            self._primary_server_node.database))

            with requests.session() as session:
                raven_command.headers.update(self.headers)
                data = json.dumps(raven_command.data, default=self.convention.json_default_method)
                response = session.request(raven_command.method, url=raven_command.url, data=data,
                                           headers=raven_command.headers)
                if response.status_code == 412 or response.status_code == 401:
                    try:
                        oauth_source = response.headers.__getitem__("OAuth-Source")
                    except KeyError:
                        raise exceptions.InvalidOperationException(
                            "Something is not right please check your server settings (do you use the right api_key)")
                    self.do_auth_request(self.server_node.api_key, oauth_source)
                    continue
                if (response.status_code == 503 or response.status_code == 502) and \
                        not self.replication_topology.empty() and not (
                                raven_command.path == "replication/topology" or "Hilo" in raven_command.path):
                    if self.primary:
                        if self.convention.failover_behavior == Failover.fail_immediately or force_read_from_master:
                            raise exceptions.ErrorResponseException("Failed to get response from server")
                        self.primary = False
                        self.is_alive(
                            {"url": self._primary_server_node.url, "database": self._primary_server_node.database},
                            primary=True)

                    else:
                        with self.lock:
                            if not index:
                                index = 0
                            peek_item = self.replication_topology.peek(index)
                            if self.url == peek_item["url"] and self.database == peek_item["database"]:
                                self.is_alive(self.replication_topology.get(index))
                    if self.replication_topology.empty():
                        raise exceptions.ErrorResponseException("Please check your databases")
                    self.server_node = self.replication_topology.peek()
                    continue
            return raven_command.set_response(response)

    def is_alive(self, destination, primary=False):
        with requests.session() as session:
            while True:
                response = session.request("GET",
                                           "{0}/databases/{1}/replication/topology?check-server-reachable".format(
                                               destination["url"], destination["database"]), headers=self.headers)
                if response.status_code == 412 or response.status_code == 401:
                    try:
                        try:
                            oauth_source = response.headers.__getitem__("OAuth-Source")
                        except KeyError:
                            raise exceptions.InvalidOperationException(
                                "Something is not right please check your server settings")
                        self.do_auth_request(self.api_key, oauth_source)
                    except exceptions.ErrorResponseException:
                        break
                if response.status_code == 200:
                    if primary:
                        self.primary = True
                    else:
                        self.replication_topology.put(destination)
                    return
                else:
                    break
            is_alive_timer = Timer(5, lambda: self.is_alive(destination, primary))
            is_alive_timer.daemon = True
            is_alive_timer.start()

    def check_database_exists(self, path):
        return self.execute(path, "GET", force_read_from_master=True, uri="docs")

    def call_hilo(self, type_tag_name, max_id, etag):
        headers = {"if-None-Match": etag}
        put_url = "docs/Raven%2FHilo%2F{0}".format(type_tag_name)
        response = self.execute(put_url, "PUT", data={"Max": max_id},
                                headers=headers)
        if response.status_code == 409:
            raise exceptions.FetchConcurrencyException(response.json["Error"])
        if response.status_code != 201:
            raise exceptions.ErrorResponseException("Something is wrong with the request")

    def update_replication(self, topology_file):
        with open(topology_file, 'w+') as f:
            f.write(json.dumps(self.topology))
            self.replication_topology.queue.clear()
            self._load_topology()

    def check_replication_change(self, topology_file):

        try:
            response = self.execute("replication/topology", "GET")
            if response.status_code == 200:
                topology = response.json()
                with self.lock:
                    if self.topology != topology:
                        self.topology = topology
                        self.update_replication(topology_file)
            elif response.status_code != 400 and response.status_code != 404 and not self.topology:
                if response.status_code == 500:
                    raise exceptions.DatabaseDoesNotExistException(
                        "Database '{0}' was not found".format(self._primary_server_node.database))
                raise exceptions.ErrorResponseException(
                    "Could not connect to the database {0} please check the problem".format(
                        self._primary_server_node.database))
        except exceptions.InvalidOperationException:
            pass
        if not self.database.lower() == self.convention.system_database:
            timer = Timer(60 * 5, lambda: self.check_replication_change(topology_file))
            timer.daemon = True
            timer.start()

    def get_replication_topology(self):
        with self.lock:
            hash_name = hashlib.md5(
                "{0}/{1}".format(self._primary_server_node.url, self._primary_server_node.database).encode(
                    'utf-8')).hexdigest()
            topology_file = "{0}{1}RavenDB_Replication_Information_For - {2}".format(tempfile.gettempdir(), os.path.sep,
                                                                                     hash_name)
            try:
                with open(topology_file, 'r') as f:
                    self.topology = json.loads(f.read())
                    self._load_topology()
            except IOError:
                pass

        self.check_replication_change(topology_file)

    def _load_topology(self):
        for destination in self.topology["Destinations"]:
            if not destination["Disabled"] and not destination["IgnoredClient"]:
                self.replication_topology.put(
                    ServerNode(destination["Url"], destination["Database"], destination["ApiKey"]))
                self.replication_topology.put({"url": destination["Url"], "database": destination["Database"],
                                               "credentials": {"api_key": destination["ApiKey"],
                                                               "domain": destination["Domain"]}})

    def do_auth_request(self, api_key, oauth_source, second_api_key=None):
        api_name, secret = api_key.split('/', 1)
        tries = 1
        headers = {"grant_type": "client_credentials"}
        data = None
        with requests.session() as session:
            while True:
                oath = session.request(method="POST", url=oauth_source,
                                       headers=headers, data=data)
                if oath.reason == "Precondition Failed":
                    if tries > 1:
                        if not (second_api_key and self.api_key != second_api_key and tries < 3):
                            raise exceptions.ErrorResponseException("Unauthorized")
                        api_name, secret = second_api_key.split('/', 1)
                        tries += 1

                    authenticate = oath.headers.__getitem__("www-authenticate")[len("Raven  "):]
                    challenge_dict = dict(item.split("=", 1) for item in authenticate.split(','))

                    exponent_str = challenge_dict.get("exponent", None)
                    modulus_str = challenge_dict.get("modulus", None)
                    challenge = challenge_dict.get("challenge", None)

                    exponent = bytes_to_long(base64.standard_b64decode(exponent_str))
                    modulus = bytes_to_long(base64.standard_b64decode(modulus_str))

                    rsa = RSA.construct((modulus, exponent))
                    cipher = PKCS1_OAEP.new(rsa)

                    iv = get_random_bytes(16)
                    key = get_random_bytes(32)
                    encoder = PKCS7Encoder()

                    cipher_text = cipher.encrypt(key + iv)
                    results = []
                    results.extend(cipher_text)

                    aes = AES.new(key, AES.MODE_CBC, iv)
                    sub_data = Utils.dict_to_string({"api key name": api_name, "challenge": challenge,
                                                     "response": base64.b64encode(hashlib.sha1(
                                                         '{0};{1}'.format(challenge, secret).encode(
                                                             'utf-8')).digest())})

                    results.extend(aes.encrypt(encoder.encode(sub_data)))
                    data = Utils.dict_to_string({"exponent": exponent_str, "modulus": modulus_str,
                                                 "data": base64.standard_b64encode(bytearray(results))})

                    if exponent is None or modulus is None or challenge is None:
                        raise exceptions.InvalidOperationException(
                            "Invalid response from server, could not parse raven authentication information:{0} ".format(
                                authenticate))
                    tries += 1
                elif oath.status_code == 200:
                    oath_json = oath.json()
                    body = oath_json["Body"]
                    signature = oath_json["Signature"]
                    if not sys.version_info.major > 2:
                        body = body.encode('utf-8')
                        signature = signature.encode('utf-8')
                    with self.lock:
                        self._token = "Bearer {0}".format(
                            {"Body": body, "Signature": signature})
                        self.headers.update({"Authorization": self._token})
                    break
                else:
                    raise exceptions.ErrorResponseException(oath.reason)

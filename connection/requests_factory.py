from data.document_convention import DocumentConvention, Failover
from custom_exceptions import exceptions
from tools.indexqueue import IndexQueue
import tempfile
import requests
import sys
import json
import hashlib
import os
from threading import Timer, Lock


class HttpRequestsFactory(object):
    # TODO need to do fail over behavior and to check for the master
    def __init__(self, url, database, convention=None, api_key=None):
        self.url = url
        self._primary_url = url
        self.database = database
        self._primary_database = database
        self.primary = False
        self.api_key = api_key
        self.version_info = sys.version_info.major
        self.convention = convention
        if self.convention is None:
            self.convention = DocumentConvention()
        self.headers = {"Accept": "application/json", "Has-Api-key": True if self.api_key is not None else False,
                        "Raven-Client-Version": "3.0.0.0"}
        self.replication_topology = IndexQueue()
        self.topology_change_counter = 0
        self.lock = Lock()
        self.topology = None
        self.request_count = 0

    def http_request_handler(self, path, method, data=None, headers=None, admin=False):
        if self.url == self._primary_url and self.database == self._primary_database and not self.primary:
            self.primary = True

        return self._execute_with_replication(path, method, headers=headers, data=data, admin=admin)

    def _execute_with_replication(self, path, method, headers, data=None, admin=False):
        while True:
            index = None
            url = ""
            if not admin:
                if method == "GET":
                    if path == "replication/topology" or "Hilo" in path:
                        if not self.primary:
                            raise exceptions.InvalidOperationException(
                                "Cant get access to {0} when {1}(primary) is Down".format(path, self._primary_database))
                    elif self.convention.failover_behavior == Failover.read_from_all_servers:
                        with self.lock:
                            self.request_count += 1
                            index = self.request_count % (len(self.replication_topology) + 1)
                        if index != 0 or not self.primary:  # if 0 use the primary
                            index -= 1
                            destination = self.replication_topology.peek(index if index > 0 else 0)
                            url = "{0}/databases/{1}/{2}".format(destination["url"], destination["database"], path)
                    elif self.convention.failover_behavior == Failover.allow_reads_from_secondaries:
                        url = "{0}/databases/{1}/{2}".format(self.url, self.database, path)
                else:
                    if not self.primary:
                        if self.convention.failover_behavior == \
                                Failover.allow_reads_from_secondaries_and_writes_to_secondaries:
                            url = "{0}/databases/{1}/{2}".format(self.url, self.database, path)
                        else:
                            raise exceptions.InvalidOperationException(
                                "Cant write to server when the primary is down when failover = {0}".format(
                                    self.convention.failover_behavior.name))

            else:
                url = "{0}/admin/databases/{1}".format(self._primary_database, path)

            if not url:
                url = "{0}/databases/{1}/{2}".format(self._primary_url, self._primary_database, path)
            with requests.session() as session:
                headers = headers.update(self.headers) if headers else self.headers
                response = session.request(method, url=url, json=data, headers=headers)
                if response.status_code == 503 and not self.replication_topology.empty():
                    if self.primary:
                        if self.convention.failover_behavior == Failover.fail_immediately:
                            raise exceptions.ErrorResponseException("Failed to get response from server")
                        self.primary = False
                        self.is_alive({"url": self._primary_url, "database": self._primary_database}, primary=True)

                    else:
                        with self.lock:
                            if not index:
                                index = 0
                            peek_item = self.replication_topology.peek(index)
                            if self.url == peek_item["url"] and self.database == peek_item["database"]:
                                self.is_alive(self.replication_topology.get(index))
                    if self.replication_topology.empty():
                        raise exceptions.ErrorResponseException("Please check your databases")
                    destination = self.replication_topology.peek()
                    self.database = destination["database"]
                    self.url = destination["url"]
                    continue
            return response

    def is_alive(self, destination, primary=False):
        with requests.session() as session:
            response = session.request("GET", "{0}/databases/{1}/replication/topology?check-server-reachable".format(
                destination["url"], destination["database"]))
            if response.status_code == 200:
                if primary:
                    self.primary = True
                else:
                    self.replication_topology.put(destination)
                return
        Timer(5, self.is_alive, [destination, primary])

    def database_open_request(self, path):
        url = "{0}/docs?{1}".format(self.url, path)
        with requests.session() as session:
            return session.request("GET", url)

    def call_hilo(self, type_tag_name, max_id, etag):
        headers = {"if-None-Match": etag}
        put_url = "docs/Raven%2FHilo%2F{0}".format(type_tag_name)
        response = self.http_request_handler(put_url, "PUT", data={"Max": max_id},
                                             headers=headers)
        if response.status_code == 409:
            raise exceptions.FetchConcurrencyException(response.json["Error"])
        if response.status_code != 201:
            raise exceptions.ErrorResponseException("Something is wrong with the request")

    def update_replication(self, topology_file):
        with open(topology_file, 'a+') as f:
            f.write(json.dumps(self.topology))
        self.replication_topology.put((0, {"url": self._primary_url, "database": self._primary_database}))
        for destination in self.topology["Destinations"]:
            self.replication_topology.put({"url": destination["Url"], "database": destination["Database"],
                                           "credentials": {"api_key": destination["ApiKey"],
                                                           "domain": destination["Domain"]}})

    def check_replication_change(self, topology_file):
        response = self.http_request_handler("replication/topology", "GET")
        if response.status_code == 200:
            topology = response.json()
            if self.topology != topology:
                self.topology = topology
                self.update_replication(topology_file)
            elif response.status_code == 400:
                pass
            else:
                raise exceptions.ErrorResponseException(
                    "Could not connect to the database {0} please check the problem".format(self._primary_database))
            Timer(60 * 5, self.check_replication_change, [topology_file])

    def get_replication_topology(self):
        with self.lock:
            hash_name = hashlib.md5(
                "{0}/{1}".format(self._primary_url, self._primary_database).encode('utf-8')).hexdigest()
            topology_file = "{0}\\RavenDB_Replication_Information_For - {1}".format(tempfile.gettempdir(), hash_name)
            try:
                with open(topology_file, 'r') as f:
                    self.topology = json.loads(f.read())
                    for destination in self.topology["Destinations"]:
                        self.replication_topology.put({"url": destination["Url"], "database": destination["Database"],
                                                       "credentials": {"api_key": destination["ApiKey"],
                                                                       "domain": destination["Domain"]}})
            except IOError:
                self.check_replication_change(topology_file)

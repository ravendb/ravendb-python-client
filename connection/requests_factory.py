from data.document_convention import DocumentConvention
from custom_exceptions import exceptions
from tools.queue import PriorityQueue
import tempfile
import requests
import sys
import json
import hashlib
import os
from threading import Timer, Lock


class ReplicationDestination(object):
    def __init__(self, url, database, credentials, priority=0):
        self.url = url
        self.database = database
        self.credentials = credentials
        self.priority = priority


class HttpRequestsFactory(object):
    # TODO need to do fail over behavior and to check for the master
    def __init__(self, url, database, convention=None, api_key=None):
        self.url = url
        self._primary_url = url
        self.database = database
        self._primary_database = database
        self.primary = None
        self.api_key = api_key
        self.version_info = sys.version_info.major
        self.convention = convention
        if self.convention is None:
            self.convention = DocumentConvention()
        self.headers = {"Accept": "application/json", "Has-Api-key": True if self.api_key is not None else False,
                        "Raven-Client-Version": "3.0.0.0"}
        self.replication_topology = PriorityQueue()
        self.topology_change_counter = 0
        self.lock = Lock()

    def http_request_handler(self, path, method, data=None, headers=None, admin=False):
        if headers is not None:
            headers.update(self.headers)
        else:
            headers = self.headers
        with requests.session() as session:
            while True:
                if self._primary_database != self.database or self._primary_url != self.url:
                    response = session.request("GET",
                                               "{0}/databases/{1}/replication/topology?check-server-reachable".format(
                                                   self._primary_url, self._primary_database))
                    if response.status_code == 200:
                        self.replication_topology.put(self.primary)
                        self.execute_with_replication()

                if admin:
                    url = "{0}/admin/databases/{1}".format(self.url, path)
                else:
                    url = "{0}/databases/{1}/{2}".format(self.url, self.database, path)
                response = session.request(method, url=url, json=data, headers=headers)
                if response.status_code == 503 and not self.replication_topology.empty():
                    self.replication_topology.get()
                    self.execute_with_replication()
                    continue
                return response

    def execute_with_replication(self):
        if self.replication_topology.empty():
            raise exceptions.ErrorResponseException("Please check your databases")
        destination = self.replication_topology.peek()[1]
        self.database = destination["database"]
        self.url = destination["url"]

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

    def get_replication_topology(self):
        with self.lock:
            hash_name = hashlib.md5("I am going home".encode('utf-8')).hexdigest()
            topology_file = "{0}\\RavenDB_Replication_Information_For - {1}".format(tempfile.gettempdir(), hash_name)
            try:
                with open(topology_file, 'r') as f:
                    topology = json.loads(f.read())
                    self.primary = (0, {"url": self._primary_url, "database": self._primary_database})
                    self.replication_topology.put(self.primary)
                    priority = 1
                    for destination in topology["Destinations"]:
                        self.replication_topology.put(
                            (priority, {"url": destination["Url"], "database": destination["Database"],
                                        "credentials": {"api_key": destination["ApiKey"],
                                                        "domain": destination["Domain"]}}))
                        priority += 1
            except IOError:
                response = self.http_request_handler("replication/topology", "GET")
                if response.status_code == 200:
                    topology = response.json()
                    with open(topology_file, 'a+') as f:
                        f.write(json.dumps(topology))
                    self.replication_topology.put((0, {"url": self._primary_url, "database": self._primary_database}))
                    priority = 1
                    for destination in topology["Destinations"]:
                        self.replication_topology.put(
                            (priority, {"url": destination["Url"], "database": destination["Database"],
                                        "credentials": {"api_key": destination["ApiKey"],
                                                        "domain": destination["Domain"]}}))
                        priority += 1
                elif response.status_code == 400:
                    pass
                else:
                    raise exceptions.ErrorResponseException(
                        "Could not connect to the database {0} please check the problem".format(self._primary_database))

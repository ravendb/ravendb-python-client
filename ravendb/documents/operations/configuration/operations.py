from __future__ import annotations

import json
from typing import TYPE_CHECKING

import requests

from ravendb.documents.operations.configuration.definitions import ClientConfiguration
from ravendb.documents.operations.definitions import VoidMaintenanceOperation, MaintenanceOperation
from ravendb.http.raven_command import RavenCommand, VoidRavenCommand
from ravendb.http.server_node import ServerNode
from ravendb.http.topology import RaftCommand
from ravendb.util.util import RaftIdGenerator

if TYPE_CHECKING:
    from ravendb.documents.conventions import DocumentConventions

from ravendb.serverwide.operations.common import ServerOperation, VoidServerOperation


class GetClientConfigurationOperation(MaintenanceOperation):
    def get_command(self, conventions: "DocumentConventions") -> "RavenCommand[GetClientConfigurationOperation.Result]":
        return self.GetClientConfigurationCommand()

    class GetClientConfigurationCommand(RavenCommand):
        def __init__(self):
            super().__init__(GetClientConfigurationOperation.Result)

        def is_read_request(self) -> bool:
            return False

        def create_request(self, node: ServerNode) -> requests.Request:
            return requests.Request(method="GET", url=f"{node.url}/databases/{node.database}/configuration/client")

        def set_response(self, response: str, from_cache: bool) -> None:
            if response is None:
                return
            self.result = GetClientConfigurationOperation.Result.from_json(json.loads(response))

    class Result:
        def __init__(self, etag: int, configuration: ClientConfiguration):
            self.etag = etag
            self.configuration = configuration

        @classmethod
        def from_json(cls, json_dict: dict) -> GetClientConfigurationOperation.Result:
            return cls(json_dict["Etag"], ClientConfiguration.from_json(json_dict["Configuration"]))


class PutClientConfigurationOperation(VoidMaintenanceOperation):
    def __init__(self, config: ClientConfiguration):
        if config is None:
            raise ValueError("Configuration cannot be None")
        self.__configuration = config

    def get_command(self, conventions: "DocumentConventions") -> "VoidRavenCommand":
        return self.__PutClientConfigurationCommand(conventions, self.__configuration)

    class __PutClientConfigurationCommand(VoidRavenCommand, RaftCommand):
        def __init__(self, conventions: "DocumentConventions", config: ClientConfiguration):
            super().__init__()
            if conventions is None:
                raise ValueError("Conventions cannot be None")

            if config is None:
                raise ValueError("Configuration cannot be None")

            self.__configuration = json.dumps(config.to_json(), default=conventions.json_default_method)

        def create_request(self, node: ServerNode) -> requests.Request:
            return requests.Request(
                "PUT", f"{node.url}/databases/{node.database}/admin/configuration/client", data=self.__configuration
            )

        def get_raft_unique_request_id(self) -> str:
            return RaftIdGenerator.new_id()


class PutServerWideClientConfigurationOperation(VoidServerOperation):
    def __init__(self, config: ClientConfiguration):
        if config is None:
            raise ValueError("Configuration cannot be None")
        self.__configuration = config

    def get_command(self, conventions: "DocumentConventions") -> "VoidRavenCommand":
        return self.__PutServerWideClientConfigurationCommand(conventions, self.__configuration)

    class __PutServerWideClientConfigurationCommand(VoidRavenCommand, RaftCommand):
        def __init__(self, conventions: "DocumentConventions", config: ClientConfiguration):
            super().__init__()
            if conventions is None:
                raise ValueError("Conventions cannot be None")

            if config is None:
                raise ValueError("Configuration cannot be None")

            self.__configuration = json.dumps(config.to_json(), default=conventions.json_default_method)

        def create_request(self, node: ServerNode) -> requests.Request:
            return requests.Request("PUT", f"{node.url}/admin/configuration/client", data=self.__configuration)

        def get_raft_unique_request_id(self) -> str:
            return RaftIdGenerator.new_id()


class GetServerWideClientConfigurationOperation(ServerOperation[ClientConfiguration]):
    def get_command(self, conventions: "DocumentConventions") -> RavenCommand[ClientConfiguration]:
        return GetServerWideClientConfigurationOperation.__GetServerWideClientConfigurationCommand()

    class __GetServerWideClientConfigurationCommand(RavenCommand[ClientConfiguration]):
        def __init__(self):
            super().__init__(ClientConfiguration)

        def is_read_request(self) -> bool:
            return False

        def create_request(self, node: ServerNode) -> requests.Request:
            return requests.Request(method="GET", url=f"{node.url}/configuration/client")

        def set_response(self, response: str, from_cache: bool) -> None:
            if response is None:
                return
            self.result = ClientConfiguration.from_json(json.loads(response))

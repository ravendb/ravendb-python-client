from __future__ import annotations

import json
from enum import Enum
from typing import Union, Optional, TYPE_CHECKING

import requests

from pyravendb.documents.operations.definitions import VoidMaintenanceOperation
from pyravendb.http.raven_command import RavenCommand, VoidRavenCommand
from pyravendb.http.server_node import ServerNode
from pyravendb.http.topology import RaftCommand
from pyravendb.util.util import RaftIdGenerator

if TYPE_CHECKING:
    from pyravendb.http.misc import ReadBalanceBehavior, LoadBalanceBehavior
    from pyravendb.documents.conventions.document_conventions import DocumentConventions


class StudioEnvironment(Enum):
    NONE = "NONE"
    DEVELOPMENT = "DEVELOPMENT"
    TESTING = "TESTING"
    PRODUCTION = "PRODUCTION"


class StudioConfiguration:
    def __init__(self, disabled: Optional[bool] = None, environment: Optional[StudioEnvironment] = None):
        self.disabled = disabled
        self.environment = environment


class ClientConfiguration:
    def __init__(self):
        self.__identity_parts_separator: Union[None, str] = None
        self.etag: int = 0
        self.disabled: bool = False
        self.max_number_of_requests_per_session: Union[None, int] = None
        self.read_balance_behavior: Union[None, "ReadBalanceBehavior"] = None
        self.load_balance_behavior: Union[None, "LoadBalanceBehavior"] = None
        self.load_balancer_context_seed: Union[None, int] = None

    @property
    def identity_parts_separator(self) -> str:
        return self.__identity_parts_separator

    @identity_parts_separator.setter
    def identity_parts_separator(self, value: str):
        if value is not None and "|" == value:
            raise ValueError("Cannot set identity parts separator to '|'")
        self.__identity_parts_separator = value

    def to_json(self) -> dict:
        return {
            "IdentityPartsSeparator": self.__identity_parts_separator,
            "Etag": self.etag,
            "Disabled": self.disabled,
            "MaxNumberOfRequestsPerSession": self.max_number_of_requests_per_session,
            "ReadBalanceBehavior": self.read_balance_behavior,
            "LoadBalanceBehavior": self.load_balance_behavior,
            "LoadBalancerContextSeed": self.load_balancer_context_seed,
        }

    @staticmethod
    def from_json(json_dict: dict) -> ClientConfiguration:
        config = ClientConfiguration()
        config.__identity_parts_separator = json_dict["IdentityPartsSeparator"]
        config.etag = json_dict["Etag"]
        config.disabled = json_dict["Disabled"]
        config.max_number_of_requests_per_session = json_dict["MaxNumberOfRequestsPerSession"]
        config.read_balance_behavior = json_dict["ReadBalanceBehavior"]
        config.load_balance_behavior = json_dict["LoadBalanceBehavior"]
        config.load_balancer_context_seed = json_dict["LoadBalancerContextSeed"]

        return config


class GetClientConfigurationOperation:
    def get_command(self, **kwargs) -> RavenCommand:
        return self.GetClientConfigurationCommand()

    class GetClientConfigurationCommand(RavenCommand):
        def __init__(self):
            super().__init__(GetClientConfigurationOperation.Result)

        def is_read_request(self) -> bool:
            return False

        def create_request(self, node: ServerNode) -> (requests.Request):
            return requests.Request(method="GET", url=f"{node.url}/databases/{node.database}/configuration/client")

        def set_response(self, response: str, from_cache: bool) -> None:
            if response is None:
                return
            self.result = GetClientConfigurationOperation.Result.from_json(json.loads(response))

    class Result:
        def __init__(self, etag: int, configuration: ClientConfiguration):
            self.etag = etag
            self.configuration = configuration

        @staticmethod
        def from_json(json_dict: dict) -> GetClientConfigurationOperation.Result:
            return GetClientConfigurationOperation.Result(
                json_dict["Etag"], ClientConfiguration.from_json(json_dict["Configuration"])
            )


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

            self.__configuration = json.dumps(config.to_json())

        def create_request(self, node: ServerNode) -> requests.Request:
            return requests.Request(
                "PUT", f"{node.url}/databases/{node.database}/admin/configuration/client", data=self.__configuration
            )

        def raft_unique_request_id(self) -> str:
            return RaftIdGenerator.new_id()

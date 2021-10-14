from __future__ import annotations

import json
from typing import Union

import requests

from pyravendb.data.document_conventions import DocumentConventions


class ClientConfiguration:
    def __init__(self):
        self.__identity_parts_separator: Union[None, str] = None
        self.etag: Union[None, int] = None
        self.disabled: Union[None, bool] = None
        self.max_number_of_requests_per_session: Union[None, int] = None
        self.read_balance_behavior: Union[None, ReadBalanceBehavior] = None
        self.load_balance_behavior: Union[None, LoadBalanceBehavior] = None
        self.load_balancer_context_seed: Union[None, int] = None

    @property
    def identity_parts_separator(self) -> str:
        return self.__identity_parts_separator

    @identity_parts_separator.setter
    def identity_parts_separator(self, value: str):
        if len(value) != 1:
            raise ValueError("Separator length must be 1")
        if value is not None and "|" == value:
            raise ValueError("Cannot set identity parts separator to '|'")
        self.__identity_parts_separator = value


class GetClientConfigurationOperation(MaintenanceOperation):
    def get_command(self, conventions: DocumentConventions) -> RavenCommand:
        return self.GetClientConfigurationCommand()

    class GetClientConfigurationCommand(RavenCommand):
        def __init__(self):
            super().__init__(GetClientConfigurationOperation.Result)

        def is_read_request(self) -> bool:
            return False

        def create_request(self, node: ServerNode) -> (requests.Request, str):
            return requests.Request(method="GET", url=f"{node.url}/databases/{node.database}/configuration/client")

        def set_response(self, response: str, from_cache: bool) -> None:
            if response is None:
                return
            self.result = Utils.initialize_object(json.loads(response), self._result_class)

    class Result:
        def __init__(self, etag: int, configuration: ClientConfiguration):
            self.etag = etag
            self.configuration = configuration

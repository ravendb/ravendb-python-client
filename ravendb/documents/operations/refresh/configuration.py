from __future__ import annotations

import json
from typing import Optional, TYPE_CHECKING, Dict
import requests
from ravendb.http.topology import RaftCommand
from ravendb.documents.operations.definitions import MaintenanceOperation
from ravendb.util.util import RaftIdGenerator
from ravendb.http.raven_command import RavenCommand

if TYPE_CHECKING:
    from ravendb.documents.conventions import DocumentConventions
    from ravendb.http.server_node import ServerNode


class RefreshConfiguration:
    def __init__(self, disabled: Optional[bool] = None, refresh_frequency_in_sec: Optional[int] = None):
        self.disabled = disabled
        self.refresh_frequency_in_sec = refresh_frequency_in_sec

    def to_json(self) -> Dict:
        return {"Disabled": self.disabled, "RefreshFrequencyInSec": self.refresh_frequency_in_sec}


class ConfigureRefreshOperationResult:
    def __init__(self, raft_command_index: int):
        self.raft_command_index = raft_command_index

    @classmethod
    def from_json(cls, json_dict: Dict) -> ConfigureRefreshOperationResult:
        return cls(json_dict["RaftCommandIndex"])


class ConfigureRefreshOperation(MaintenanceOperation[ConfigureRefreshOperationResult]):
    def __init__(self, configuration: RefreshConfiguration):
        if configuration is None:
            raise ValueError("Configuration cannot be None")
        self._configuration = configuration

    def get_command(self, conventions: "DocumentConventions") -> "RavenCommand[ConfigureRefreshOperationResult]":
        return self._ConfigureRefreshCommand(self._configuration)

    class _ConfigureRefreshCommand(RavenCommand[ConfigureRefreshOperationResult], RaftCommand):
        def __init__(self, configuration: RefreshConfiguration):
            super(ConfigureRefreshOperation._ConfigureRefreshCommand, self).__init__(ConfigureRefreshOperationResult)
            self._configuration = configuration

        def is_read_request(self) -> bool:
            return False

        def create_request(self, node: ServerNode) -> requests.Request:
            url = f"{node.url}/databases/{node.database}/admin/refresh/config"
            request = requests.Request("POST", data=self._configuration.to_json())
            request.url = url
            return request

        def set_response(self, response: str, from_cache: bool) -> None:
            if response is None:
                self._throw_invalid_response()

            self.result = ConfigureRefreshOperationResult.from_json(json.loads(response))

        def get_raft_unique_request_id(self) -> str:
            return RaftIdGenerator.new_id()

from __future__ import annotations
import json
from typing import Optional, Dict, Any

import requests

from ravendb import ServerNode
from ravendb.http.raven_command import RavenCommand
from ravendb.documents.conventions import DocumentConventions
from ravendb.documents.operations.expiration.configuration import ExpirationConfiguration
from ravendb.documents.operations.definitions import MaintenanceOperation


class ConfigureExpirationOperationResult:
    def __init__(self, raft_command_index: int = None):
        self.raft_command_index = raft_command_index

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> ConfigureExpirationOperationResult:
        return cls(json_dict["RaftCommandIndex"])


class ConfigureExpirationOperation(MaintenanceOperation[ConfigureExpirationOperationResult]):
    def __init__(self, configuration: ExpirationConfiguration):
        self._configuration = configuration

    def get_command(self, conventions: DocumentConventions) -> RavenCommand:
        return self.ConfigureExpirationCommand(self._configuration)

    class ConfigureExpirationCommand(RavenCommand[ConfigureExpirationOperationResult]):
        def __init__(self, configuration: ExpirationConfiguration):
            super().__init__(ConfigureExpirationOperationResult)
            if configuration is None:
                raise ValueError("Configuration cannot be None")
            self._configuration = configuration

        def is_read_request(self) -> bool:
            return False

        def create_request(self, node: ServerNode) -> requests.Request:
            url = f"{node.url}/databases/{node.database}/admin/expiration/config"

            request = requests.Request("POST", url)
            request.data = self._configuration.to_json()

            return request

        def set_response(self, response: Optional[str], from_cache: bool) -> None:
            if response is None:
                self._throw_invalid_response()

            self.result = ConfigureExpirationOperationResult.from_json(json.loads(response))

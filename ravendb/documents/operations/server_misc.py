from __future__ import annotations

import json
from typing import Dict, Any, List, Optional

import requests

from ravendb import RavenCommand, RaftCommand, ServerNode
from ravendb.serverwide.operations.common import ServerOperation, T
from ravendb.util.util import RaftIdGenerator


class DisableDatabaseToggleResult:
    def __init__(self, disabled: bool = None, name: str = None, success: bool = None, reason: str = None) -> None:
        self.disabled = disabled
        self.name = name
        self.success = success
        self.reason = reason

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> "DisableDatabaseToggleResult":
        return cls(json_dict["Disabled"], json_dict["Name"], json_dict["Success"], json_dict["Reason"])


class ToggleDatabasesStateOperation(ServerOperation[DisableDatabaseToggleResult]):
    class Parameters:
        def __init__(self, databases_names: List[str] = None):
            self.databases_names = databases_names

        def to_json(self) -> Dict[str, Any]:
            return {"DatabaseNames": self.databases_names}

    def __init__(self, database_name: str, disable: bool):
        if database_name is None:
            raise ValueError("Database name cannot be None")

        self._disable = disable
        self._parameters = self.Parameters()
        self._parameters.databases_names = [database_name]

    @classmethod
    def from_multiple_names(cls, database_names: List[str], disable: bool):
        this = cls("", disable)
        this._parameters = cls.Parameters(database_names)
        return this

    @classmethod
    def from_parameters(cls, parameters: ToggleDatabasesStateOperation.Parameters, disable: bool):
        if parameters is None:
            raise ValueError("Parameters cannot be None")

        if not parameters.databases_names:
            raise ValueError("Parameters.DatabaseNames cannot be None or empty")

        this = cls("", disable)
        this._parameters = parameters
        return this

    def get_command(self, conventions: "DocumentConventions") -> RavenCommand[T]:
        return self.ToggleDatabaseStateCommand(self._parameters, self._disable)

    class ToggleDatabaseStateCommand(RavenCommand[DisableDatabaseToggleResult], RaftCommand):
        def __init__(self, parameters: ToggleDatabasesStateOperation.Parameters, disable: bool):
            super().__init__(DisableDatabaseToggleResult)

            if parameters is None:
                raise ValueError("Parameters cannot be None")

            self._parameters = parameters
            self._disable = disable

        def create_request(self, node: ServerNode) -> requests.Request:
            toggle = "disable" if self._disable else "enable"
            url = f"{node.url}/admin/databases/{toggle}"

            request = requests.Request(method="POST", url=url, data=self._parameters.to_json())

            return request

        def set_response(self, response: Optional[str], from_cache: bool) -> None:
            if response is None:
                self._throw_invalid_response()

            json_node = json.loads(response)
            status = json_node.get("Status")

            if not isinstance(status, list):
                self._throw_invalid_response()

            if not status:
                return None

            databases_status = status[0]
            self.result = DisableDatabaseToggleResult.from_json(databases_status)

        def is_read_request(self) -> bool:
            return False

        def get_raft_unique_request_id(self) -> str:
            return RaftIdGenerator.new_id()

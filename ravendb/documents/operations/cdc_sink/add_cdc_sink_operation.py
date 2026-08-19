from __future__ import annotations

import json
from typing import Any, Dict, TYPE_CHECKING

import requests

from ravendb.documents.conventions import DocumentConventions
from ravendb.documents.operations.definitions import MaintenanceOperation
from ravendb.http.raven_command import RavenCommand
from ravendb.http.server_node import ServerNode
from ravendb.http.topology import RaftCommand
from ravendb.util.util import RaftIdGenerator

if TYPE_CHECKING:
    from ravendb.documents.operations.cdc_sink.cdc_sink_configuration import CdcSinkConfiguration


class AddCdcSinkOperationResult:
    def __init__(self, raft_command_index: int = 0, task_id: int = 0):
        self.raft_command_index = raft_command_index
        self.task_id = task_id

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> "AddCdcSinkOperationResult":
        return cls(
            raft_command_index=json_dict.get("RaftCommandIndex", 0),
            task_id=json_dict.get("TaskId", 0),
        )


class AddCdcSinkOperation(MaintenanceOperation[AddCdcSinkOperationResult]):
    def __init__(self, configuration: "CdcSinkConfiguration"):
        if configuration is None:
            raise ValueError("configuration cannot be None")
        self._configuration = configuration

    def get_command(self, conventions: DocumentConventions) -> RavenCommand[AddCdcSinkOperationResult]:
        return AddCdcSinkCommand(conventions, self._configuration)


class AddCdcSinkCommand(RavenCommand[AddCdcSinkOperationResult], RaftCommand):
    def __init__(self, conventions: DocumentConventions, configuration: "CdcSinkConfiguration"):
        super().__init__(AddCdcSinkOperationResult)
        self._conventions = conventions
        self._configuration = configuration

    def is_read_request(self) -> bool:
        return False

    def create_request(self, node: ServerNode) -> requests.Request:
        url = f"{node.url}/databases/{node.database}/admin/cdc-sink"

        request = requests.Request("PUT", url)
        request.headers = {"Content-Type": "application/json"}
        request.data = json.dumps(self._configuration.to_json())
        return request

    def set_response(self, response: str, from_cache: bool) -> None:
        if response is None:
            self._throw_invalid_response()

        self.result = AddCdcSinkOperationResult.from_json(json.loads(response))

    def get_raft_unique_request_id(self) -> str:
        return RaftIdGenerator.new_id()

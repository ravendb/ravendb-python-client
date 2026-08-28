from __future__ import annotations

import json
from typing import TYPE_CHECKING, Optional

import requests

from ravendb.documents.operations.cdc_sink.configuration import CdcSinkConfiguration
from ravendb.documents.operations.definitions import MaintenanceOperation
from ravendb.documents.operations.etl.etl_operation_results import AddEtlOperationResult, UpdateEtlOperationResult
from ravendb.http.raven_command import RavenCommand
from ravendb.http.server_node import ServerNode
from ravendb.http.topology import RaftCommand
from ravendb.util.util import RaftIdGenerator

if TYPE_CHECKING:
    from ravendb.documents.conventions import DocumentConventions


class AddCdcSinkOperation(MaintenanceOperation[AddEtlOperationResult]):
    def __init__(self, configuration: CdcSinkConfiguration):
        if configuration is None:
            raise ValueError("configuration cannot be None")
        self._configuration = configuration

    def get_command(self, conventions: "DocumentConventions") -> RavenCommand[AddEtlOperationResult]:
        return AddCdcSinkOperation._AddCdcSinkCommand(self._configuration)

    class _AddCdcSinkCommand(RavenCommand[AddEtlOperationResult], RaftCommand):
        def __init__(self, configuration: CdcSinkConfiguration):
            super().__init__(AddEtlOperationResult)
            self._configuration = configuration

        def is_read_request(self) -> bool:
            return False

        def create_request(self, node: ServerNode) -> requests.Request:
            url = f"{node.url}/databases/{node.database}/admin/cdc-sink"
            request = requests.Request("PUT", url)
            request.data = self._configuration.to_json()
            return request

        def set_response(self, response: Optional[str], from_cache: bool) -> None:
            if response is None:
                self._throw_invalid_response()
            self.result = AddEtlOperationResult.from_json(json.loads(response))

        def get_raft_unique_request_id(self) -> str:
            return RaftIdGenerator.new_id()


class UpdateCdcSinkOperation(MaintenanceOperation[UpdateEtlOperationResult]):
    # The server applies an update as a delete followed by an add, so the task id changes.
    def __init__(self, task_id: int, configuration: CdcSinkConfiguration):
        if configuration is None:
            raise ValueError("configuration cannot be None")
        self._task_id = task_id
        self._configuration = configuration

    def get_command(self, conventions: "DocumentConventions") -> RavenCommand[UpdateEtlOperationResult]:
        return UpdateCdcSinkOperation._UpdateCdcSinkCommand(self._task_id, self._configuration)

    class _UpdateCdcSinkCommand(RavenCommand[UpdateEtlOperationResult], RaftCommand):
        def __init__(self, task_id: int, configuration: CdcSinkConfiguration):
            super().__init__(UpdateEtlOperationResult)
            self._task_id = task_id
            self._configuration = configuration

        def is_read_request(self) -> bool:
            return False

        def create_request(self, node: ServerNode) -> requests.Request:
            url = f"{node.url}/databases/{node.database}/admin/cdc-sink?id={self._task_id}"
            request = requests.Request("PUT", url)
            request.data = self._configuration.to_json()
            return request

        def set_response(self, response: Optional[str], from_cache: bool) -> None:
            if response is None:
                self._throw_invalid_response()
            self.result = UpdateEtlOperationResult.from_json(json.loads(response))

        def get_raft_unique_request_id(self) -> str:
            return RaftIdGenerator.new_id()

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import requests

from ravendb.documents.operations.definitions import MaintenanceOperation
from ravendb.documents.operations.queue_sink.azure_service_bus_sink_source import AzureServiceBusSinkSource
from ravendb.documents.operations.queue_sink.configuration import (
    AddQueueSinkOperationResult,
    QueueSinkConfiguration,
    QueueSinkProcessState,
    QueueSinkScript,
    UpdateQueueSinkOperationResult,
)
from ravendb.http.raven_command import RavenCommand
from ravendb.http.server_node import ServerNode
from ravendb.http.topology import RaftCommand
from ravendb.util.util import RaftIdGenerator

if TYPE_CHECKING:
    from ravendb.documents.conventions import DocumentConventions


class AddQueueSinkOperation(MaintenanceOperation[AddQueueSinkOperationResult]):
    """
    Adds a queue sink task, which has RavenDB consume messages from an external broker
    such as Kafka or RabbitMQ and store them in the database.
    """

    def __init__(self, configuration: QueueSinkConfiguration):
        if configuration is None:
            raise ValueError("configuration cannot be None")

        self._configuration = configuration

    def get_command(self, conventions: "DocumentConventions") -> RavenCommand[AddQueueSinkOperationResult]:
        return self._AddQueueSinkCommand(self._configuration)

    class _AddQueueSinkCommand(RavenCommand[AddQueueSinkOperationResult], RaftCommand):
        def __init__(self, configuration: QueueSinkConfiguration):
            super().__init__(AddQueueSinkOperationResult)
            self._configuration = configuration

        def is_read_request(self) -> bool:
            return False

        def create_request(self, node: ServerNode) -> requests.Request:
            url = f"{node.url}/databases/{node.database}/admin/queue-sink"

            request = requests.Request("PUT", url)
            request.data = self._configuration.to_json()
            return request

        def set_response(self, response: str, from_cache: bool) -> None:
            if response is None:
                self._throw_invalid_response()

            self.result = AddQueueSinkOperationResult.from_json(json.loads(response))

        def get_raft_unique_request_id(self) -> str:
            return RaftIdGenerator.new_id()


class UpdateQueueSinkOperation(MaintenanceOperation[UpdateQueueSinkOperationResult]):
    """Updates an existing queue sink task."""

    def __init__(self, task_id: int, configuration: QueueSinkConfiguration):
        if configuration is None:
            raise ValueError("configuration cannot be None")

        self._task_id = task_id
        self._configuration = configuration

    def get_command(self, conventions: "DocumentConventions") -> RavenCommand[UpdateQueueSinkOperationResult]:
        return self._UpdateQueueSinkCommand(self._task_id, self._configuration)

    class _UpdateQueueSinkCommand(RavenCommand[UpdateQueueSinkOperationResult], RaftCommand):
        def __init__(self, task_id: int, configuration: QueueSinkConfiguration):
            super().__init__(UpdateQueueSinkOperationResult)
            self._task_id = task_id
            self._configuration = configuration

        def is_read_request(self) -> bool:
            return False

        def create_request(self, node: ServerNode) -> requests.Request:
            url = f"{node.url}/databases/{node.database}/admin/queue-sink?id={self._task_id}"

            request = requests.Request("PUT", url)
            request.data = self._configuration.to_json()
            return request

        def set_response(self, response: str, from_cache: bool) -> None:
            if response is None:
                self._throw_invalid_response()

            self.result = UpdateQueueSinkOperationResult.from_json(json.loads(response))

        def get_raft_unique_request_id(self) -> str:
            return RaftIdGenerator.new_id()


__all__ = [
    "AddQueueSinkOperation",
    "AddQueueSinkOperationResult",
    "AzureServiceBusSinkSource",
    "QueueSinkConfiguration",
    "QueueSinkProcessState",
    "QueueSinkScript",
    "UpdateQueueSinkOperation",
    "UpdateQueueSinkOperationResult",
]

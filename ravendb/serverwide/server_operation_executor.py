from __future__ import annotations

import enum
from typing import TYPE_CHECKING, TypeVar, Optional

from ravendb.http.request_executor import ClusterRequestExecutor
from ravendb.http.topology import Topology
from ravendb.serverwide.operations.common import (
    GetBuildNumberOperation,
    ServerOperation,
    ServerWideOperation,
)
from ravendb.tools.utils import CaseInsensitiveDict

if TYPE_CHECKING:
    from ravendb.documents.store.definition import DocumentStore
    from ravendb.documents.operations.operation import Operation
    from ravendb.documents.operations.definitions import OperationIdResult

_T_OperationResult = TypeVar("_T_OperationResult")


class ConnectionStringType(enum.Enum):
    NONE = "NONE"
    RAVEN = "RAVEN"
    SQL = "SQL"
    OLAP = "OLAP"


class ServerOperationExecutor:
    def __init__(self, store: DocumentStore):
        if store is None:
            raise ValueError("Store cannot be None")
        request_executor = self.create_request_executor(store)

        if request_executor is None:
            raise ValueError("Request Executor cannot be None")
        self.__store = store
        self.__request_executor = request_executor
        self.__initial_request_executor = None
        self.__node_tag = None
        self.__cache = CaseInsensitiveDict()

        # todo: register events

        # todo: if node tag is null add after_close_listener

    def send(self, operation: ServerOperation[_T_OperationResult]) -> Optional[None, _T_OperationResult]:
        command = operation.get_command(self.__request_executor.conventions)
        self.__request_executor.execute_command(command)

        if isinstance(operation, ServerOperation):
            return command.result

    def send_async(self, operation: ServerOperation[OperationIdResult]) -> Operation:
        command = operation.get_command(self.__request_executor.conventions)

        self.__request_executor.execute_command(command)
        return ServerWideOperation(
            self.__request_executor,
            self.__request_executor.conventions,
            command.result.operation_id,
            command.selected_node_tag if command.selected_node_tag else command.result.operation_node_tag,
        )

    def close(self) -> None:
        if self.__node_tag is not None:
            return

        if self.__request_executor is not None:
            self.__request_executor.close()

        cache = self.__cache
        if cache is not None:
            for key, value in cache.items():
                request_executor = value._request_executor
                if request_executor is not None:
                    request_executor.close()

            cache.clear()

    def __get_topology(self, request_executor: ClusterRequestExecutor) -> Topology:
        topology: Topology = None
        try:
            topology = request_executor.topology
            if topology is None:
                # a bit rude way to make sure that topology was refreshed
                # but it handles a case when first topology update failed

                operation = GetBuildNumberOperation()
                command = operation.get_command(request_executor.conventions)
                request_executor.execute_command(command)

                topology = request_executor.topology

        except:
            pass

        if topology is None:
            raise RuntimeError("Could not fetch the topology")

        return topology

    @staticmethod
    def create_request_executor(store: DocumentStore) -> ClusterRequestExecutor:
        return (
            ClusterRequestExecutor.create_for_single_node(
                store.urls[0],
                store.thread_pool_executor,
                store.conventions,
                store.certificate_pem_path,
                store.trust_store_path,
            )
            if store.conventions.disable_topology_updates
            else ClusterRequestExecutor.create_without_database_name(
                store.urls,
                store.thread_pool_executor,
                store.conventions,
                store.certificate_pem_path,
                store.trust_store_path,
            )
        )

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return

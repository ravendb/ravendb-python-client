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
    NONE = "None"
    RAVEN = "Raven"
    SQL = "Sql"
    OLAP = "Olap"
    AI = "Ai"
    ELASTIC_SEARCH = "ElasticSearch"
    QUEUE = "Queue"
    SNOWFLAKE = "Snowflake"


class ServerOperationExecutor:
    def __init__(
        self,
        store: DocumentStore,
        request_executor: ClusterRequestExecutor,
        initial_request_executor: ClusterRequestExecutor = None,
        cache: CaseInsensitiveDict = None,
        node_tag: str = None,
    ):
        if store is None:
            raise ValueError("Store cannot be None")
        if request_executor is None:
            raise ValueError("Request Executor cannot be None")
        if cache is None:
            cache = CaseInsensitiveDict()

        self._store = store
        self._request_executor = request_executor
        self._initial_request_executor = initial_request_executor
        self._node_tag = node_tag
        self._cache = cache

        store.register_events_for_request_executor(self._request_executor)

        if self._node_tag is None:
            self._store.add_after_close(lambda: self._request_executor.close())

    @classmethod
    def from_store(cls, store: DocumentStore):
        return cls(store, cls.create_request_executor(store), None, CaseInsensitiveDict(), None)

    def for_node(self, node_tag: str) -> ServerOperationExecutor:
        if not node_tag or node_tag.isspace():
            raise ValueError("Value cannot be None or whitespace")

        if self._node_tag and self._node_tag.lower() == node_tag.lower():
            return self

        if self._store.conventions.disable_topology_updates:
            raise RuntimeError(
                "Cannot switch server operation executor, because conventions.disable_topology_updates is set to 'True'"
            )

        if node_tag in self._cache:
            return self._cache[node_tag]

        request_executor = self._initial_request_executor or self._request_executor
        topology = self._get_topology(self._request_executor)

        node = next((node for node in topology.nodes if node_tag.lower() == node.cluster_tag.lower()), None)

        if node is None:
            available_nodes = str.join(", ", [node.cluster_tag for node in topology.nodes])
            raise RuntimeError(f"Could not find node '{node_tag}' in the topology. Available nodes: {available_nodes}")

        cluster_executor = ClusterRequestExecutor.create_for_single_node(
            node.url,
            self._store.thread_pool_executor,
            self._store.conventions,
            self._store.certificate_pem_path,
            self._store.trust_store_path,
        )
        return ServerOperationExecutor(self._store, cluster_executor, request_executor, self._cache, node.cluster_tag)

    def send(self, operation: ServerOperation[_T_OperationResult]) -> Optional[_T_OperationResult]:
        command = operation.get_command(self._request_executor.conventions)
        self._request_executor.execute_command(command)

        if isinstance(operation, ServerOperation):
            return command.result

    def send_async(self, operation: ServerOperation[OperationIdResult]) -> Operation:
        command = operation.get_command(self._request_executor.conventions)

        self._request_executor.execute_command(command)
        return ServerWideOperation(
            self._request_executor,
            self._request_executor.conventions,
            command.result.operation_id,
            command.selected_node_tag if command.selected_node_tag else command.result.operation_node_tag,
        )

    def close(self) -> None:
        if self._node_tag is not None:
            return

        if self._request_executor is not None:
            self._request_executor.close()

        cache = self._cache
        if cache is not None:
            for key, value in cache.items():
                request_executor = value._request_executor
                if request_executor is not None:
                    request_executor.close()

            cache.clear()

    def _get_topology(self, request_executor: ClusterRequestExecutor) -> Topology:
        topology: Optional[Topology] = None
        try:
            topology = request_executor.topology
            if topology is None:
                # a bit rude way to make sure that topology was refreshed
                # but it handles a case when first topology update failed

                operation = GetBuildNumberOperation()
                command = operation.get_command(request_executor.conventions)
                request_executor.execute_command(command)

                topology = request_executor.topology

        except Exception:
            ...  # ignored

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

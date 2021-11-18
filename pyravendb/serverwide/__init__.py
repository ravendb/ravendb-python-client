from __future__ import annotations

import typing

import pyravendb.http.request_executor as req_ex
from pyravendb.tools.utils import CaseInsensitiveDict

if typing.TYPE_CHECKING:
    from pyravendb.documents import DocumentStore


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

        # todo: store.register events

        # todo: if node tag is null add after_close_listener

    def send(self, operation: ServerOperation):
        command = operation.get_command()

    def close(self):
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

    @staticmethod
    def create_request_executor(store: DocumentStore) -> req_ex.ClusterRequestExecutor:
        return (
            req_ex.ClusterRequestExecutor.create_for_single_node(
                store.urls[0], store.thread_pool_executor, store.conventions
            )
            if store.conventions.disable_topology_updates
            else req_ex.ClusterRequestExecutor.create_without_database_name(
                store.urls, store.thread_pool_executor, store.conventions
            )
        )

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return

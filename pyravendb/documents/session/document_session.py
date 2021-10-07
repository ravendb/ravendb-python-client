from __future__ import annotations

import datetime
import time
import uuid
from typing import Union, Callable, Optional

from pyravendb import constants
from pyravendb.data.document_conventions import DocumentConventions
from pyravendb.documents.commands.commands import HeadDocumentCommand, GetDocumentsCommand
from pyravendb.documents.operations.operations import BatchOperation
from pyravendb.documents.session.cluster_transaction_operation import (
    ClusterTransactionOperationsBase,
    ClusterTransactionOperations,
)
from pyravendb.documents.session.document_info import DocumentInfo
from pyravendb.documents.session.in_memory_document_session_operations import InMemoryDocumentSessionOperations
from pyravendb.documents.session.session import SessionOptions, ResponseTimeInformation
from pyravendb.http.raven_command import RavenCommand
from pyravendb.store.document_store import DocumentStore


class DocumentSession(InMemoryDocumentSessionOperations):
    def __init__(self, store: DocumentStore, key: uuid.UUID, options: SessionOptions):
        super().__init__(store, key, options)

        self.__advanced: DocumentSession._Advanced = DocumentSession._Advanced(self)
        self.__document_query_generator: DocumentSession._DocumentQueryGenerator = (
            DocumentSession._DocumentQueryGenerator(self)
        )
        self.__cluster_transaction: Union[None, ClusterTransactionOperations] = None

    def __enter__(self):
        return self

    def __exit__(self):
        pass

    @property
    def advanced(self):
        return self.__advanced

    @property
    def lazily(self):
        raise NotImplementedError()

    @property
    def cluster_session(self) -> ClusterTransactionOperationsBase:
        if self.__cluster_transaction is None:
            self.__cluster_transaction = ClusterTransactionOperations(self)
        return self.__cluster_transaction

    @property
    def has_cluster_session(self) -> bool:
        return self.__cluster_transaction is not None

    def save_changes(self) -> None:
        save_changes_operation = BatchOperation(self)
        with save_changes_operation.create_request() as command:
            if command is None:
                return

            if self.no_tracking:
                raise RuntimeError("Cannot execute save_changes when entity tracking is disabled.")

            # todo: rebuild request executor
            self.request_executor.execute(command, self._session_info)
            self.update_session_after_save_changes(command.result)
            save_changes_operation.set_result(command.result)

    def exists(self, key: str) -> bool:
        if key is None:
            raise ValueError("Key cannot be None")
        if key in self._known_missing_ids:
            return False

        if self.documents_by_id.get(key) is not None:
            return True

        command = HeadDocumentCommand(key, None)
        self.request_executor.execute(command, self._session_info)
        return command.result is not None

    def refresh(self, entity: object) -> None:
        document_info = self.documents_by_entity.get(entity)
        if document_info == None:
            raise ValueError("Cannot refresh a transient instance")
        self.increment_requests_count()

        command = GetDocumentsCommand(
            options=GetDocumentsCommand.GetDocumentsByIdsCommandOptions([document_info.key], None, False)
        )
        self.request_executor.execute(command, self._session_info)
        self._refresh_internal(entity, command, document_info)

    def _clear_cluster_session(self) -> None:
        if not self._has_cluster_session():
            return
        self.get_cluster_session().clear()

    def _generate_id(self, entity: object) -> str:
        return self.conventions.generate_document_id(self.database_name, entity)

    # todo: lazy
    def execute_all_pending_lazy_operations(self) -> ResponseTimeInformation:
        requests = []
        for i in range(len(self._pending_lazy_operations)):
            # todo: pending lazy operation create request
            req = self._pending_lazy_operations[i].create_request()
            if req is None:
                self._pending_lazy_operations.remove(i)
                i -= 1
                continue
            requests.append(req)
        if not requests:
            return ResponseTimeInformation()

        sw = datetime.datetime.now()
        response_time_duration = ResponseTimeInformation()
        while self.__execute_lazy_operations_single_step(response_time_duration, requests, sw):
            time.sleep(0.1)
        response_time_duration.compute_server_total()

        for pending_lazy_operation in self._pending_lazy_operations:
            value = self._on_evaluate_lazy.get(pending_lazy_operation)
            if value is not None:
                value(pending_lazy_operation.result)

        elapsed = datetime.datetime.now() - sw
        response_time_duration.total_client_duration = elapsed

        self._pending_lazy_operations.clear()

        return response_time_duration

    def __execute_lazy_operations_single_step(self):
        # todo: multi get operation
        multi_get_operation = None

    class _Advanced:
        def __init__(self, session: DocumentSession):
            self.__session = session

        def refresh(self, entity: object):
            pass

        def raw_query(self, object_type: type, query: str) -> RawDocumentQuery:
            pass

        def graph_query(self, object_type: type, query: str) -> GraphDocumentQuery:
            pass

        def exists(self, key: str) -> bool:
            pass

        def load_starting_with(
            self,
            object_type: type,
            id_prefix: str,
            matches: str = None,
            start: int = 0,
            page_size: int = 25,
            exclude: str = None,
            start_after: str = None,
        ) -> object:
            pass

        def load_starting_with_into_stream(
            self,
            id_prefix: str,
            output: iter,
            matches: str = None,
            start: int = 0,
            page_size: int = 25,
            exclude: str = None,
            start_after: str = None,
        ) -> None:
            pass

        def load_into_stream(self, ids: list[str], output: bytes) -> None:
            pass

        def increment(self, key_or_entity: Union[object, str], path: str, value_to_add: object) -> None:
            pass

        def patch(self, key_or_entity: Union[object, str], path: str, value: object) -> None:
            pass

        def patch_array(
            self, key_or_entity: Union[object, str], path_to_array: str, list_adder: Callable[[list], None]
        ) -> None:
            pass

        def patch_object(
            self, key_or_entity: Union[object, str], path_to_array: str, dictionary_adder: Callable[[dict], None]
        ) -> None:
            pass

        def add_or_patch(self, key: str, entity: object, path_to_object: str, value: object) -> None:
            pass

        def add_or_patch_array(
            self, key: str, entity: object, path_to_object: str, list_adder: Callable[[list], None]
        ) -> None:
            pass

        def add_or_increment(self, key: str, entity: object, path_to_object: str, val_to_add: object) -> None:
            pass

        def stream(
            self,
            query_or_raw_query: Union[RawDocumentQuery, DocumentQuery],
            stream_query_stats: Optional[StreamQueryStatistics] = None,
        ) -> iter:
            pass

        def stream_starting_with(
            self,
            object_type: type,
            starts_with: str,
            matches: str = None,
            start: int = 0,
            page_size: int = 25,
            starting_after: str = None,
        ) -> iter:
            pass

        def stream_into(self, query: Union[DocumentQuery, RawDocumentQuery], output: iter):
            pass

        def conditional_load(self, object_type: type, key: str, change_vector: str):
            pass

    class _DocumentQueryGenerator:
        def __init__(self, session: DocumentSession, conventions: DocumentConventions = None):
            self.__session = session
            self.__conventions = conventions

        @property
        def conventions(self) -> DocumentConventions:
            return self.__conventions

        @property
        def session(self) -> InMemoryDocumentSessionOperations:
            return self.__session

        def document_query(self, object_type: type, index_name: str, collection_name: str, is_map_reduce: bool):
            pass

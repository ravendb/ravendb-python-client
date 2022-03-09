from __future__ import annotations
import threading
import datetime
import uuid
from abc import abstractmethod
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, Union, Optional, TypeVar, List, Dict, TYPE_CHECKING

import requests

from pyravendb import constants, exceptions
from pyravendb.changes.database_changes import DatabaseChanges
from pyravendb.documents.operations.counters.operation import CounterOperationType
from pyravendb.documents.operations.executor import MaintenanceOperationExecutor, OperationExecutor
from pyravendb.documents.operations.indexes import PutIndexesOperation
from pyravendb.documents.session.event_args import (
    BeforeStoreEventArgs,
    AfterSaveChangesEventArgs,
    BeforeDeleteEventArgs,
    BeforeQueryEventArgs,
    SessionCreatedEventArgs,
    SessionClosingEventArgs,
    BeforeConversionToDocumentEventArgs,
    AfterConversionToDocumentEventArgs,
    BeforeConversionToEntityEventArgs,
    AfterConversionToEntityEventArgs,
    BeforeRequestEventArgs,
    SucceedRequestEventArgs,
    FailedRequestEventArgs,
)
from pyravendb.documents.store.misc import Lazy, IdTypeAndName
from pyravendb.documents.session.document_session import DocumentSession
from pyravendb.documents.session.in_memory_document_session_operations import InMemoryDocumentSessionOperations
from pyravendb.documents.session.misc import SessionOptions, DocumentQueryCustomization
from pyravendb.http.request_executor import RequestExecutor
from pyravendb.documents.identity.hilo import MultiDatabaseHiLoGenerator
from pyravendb.http.topology import Topology
from pyravendb.legacy.raven_operations.counters_operations import GetCountersOperation, CounterOperation
from pyravendb.tools.utils import CaseInsensitiveDict
from pyravendb.documents.conventions.document_conventions import DocumentConventions

T = TypeVar("T")


from pyravendb.documents.commands.batches import CommandType, CountersBatchCommandData

if TYPE_CHECKING:
    from pyravendb.documents.indexes.index_creation import AbstractIndexCreationTask, IndexCreation


class DocumentStoreBase:
    def __init__(self):
        self.__conventions = None
        self._initialized: bool = False

        self.__certificate_path: Union[None, str] = None
        self.__trust_store_path: Union[None, str] = None

        self._urls: List[str] = []
        self._database: Union[None, str] = None
        self._disposed: Union[None, bool] = None

        self.__before_store: List[Callable[[BeforeStoreEventArgs], None]] = []
        self.__after_save_changes: List[Callable[[AfterSaveChangesEventArgs], None]] = []
        self.__before_delete: List[Callable[[BeforeDeleteEventArgs], None]] = []
        self.__before_query: List[Callable[[BeforeQueryEventArgs], None]] = []
        self.__on_session_creation: List[Callable[[SessionCreatedEventArgs], None]] = []
        self.__on_session_closing: List[Callable[[SessionClosingEventArgs], None]] = []

        self.__before_conversion_to_document: List[Callable[[BeforeConversionToDocumentEventArgs], None]] = []
        self.__after_conversion_to_document: List[Callable[[AfterConversionToDocumentEventArgs], None]] = []
        self.__before_conversion_to_entity: List[Callable[[BeforeConversionToEntityEventArgs], None]] = []
        self.__after_conversion_to_entity: List[Callable[[AfterConversionToEntityEventArgs], None]] = []

        self.__before_request: List[Callable[[BeforeRequestEventArgs], None]] = []
        self.__on_succeed_request: List[Callable[[SucceedRequestEventArgs], None]] = []

        self.__on_failed_request: List[Callable[[FailedRequestEventArgs], None]] = []
        self.__on_topology_updated: List[Callable[[Topology], None]] = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    @property
    def disposed(self) -> bool:
        return self._disposed

    @property
    def conventions(self):
        if self.__conventions is None:
            self.__conventions = DocumentConventions()
        return self.__conventions

    @conventions.setter
    def conventions(self, value: DocumentConventions):
        self.__assert_not_initialized("conventions")
        self.__conventions = value

    @property
    def urls(self):
        return self._urls

    @urls.setter
    def urls(self, value: List[str]):
        self.__assert_not_initialized("urls")

        if value is None:
            raise ValueError("Value is None")

        for i in range(len(value)):
            if value[i] is None:
                raise ValueError("Urls cannot contain None")
            value[i] = str.rstrip(value[i], "/")

        self._urls = value

    @property
    def database(self) -> str:
        return self._database

    @database.setter
    def database(self, value: str):
        self.__assert_not_initialized("database")
        self._database = value

    @property
    def certificate_path(self) -> str:
        return self.__certificate_path

    @certificate_path.setter
    def certificate_path(self, value: str):
        self.__assert_not_initialized("certificate_url")
        self.__certificate_path = value

    @property
    def trust_store_path(self) -> str:
        return self.__trust_store_path

    @trust_store_path.setter
    def trust_store_path(self, value: str):
        self.__trust_store_path = value

    @abstractmethod
    def close(self):
        pass

    @abstractmethod
    def maintenance(self) -> MaintenanceOperationExecutor:
        pass

    @abstractmethod
    def operations(self) -> OperationExecutor:
        pass

    # todo: changes

    # todo: aggressive_caching

    # todo: time_series

    # todo: bulk_insert

    @abstractmethod
    def open_session(self, database: Optional[str] = None, session_options: Optional = None):
        pass

    def add_on_session_creation(self, event: Callable[[], None]):
        self.__on_session_creation.append(event)

    def remove_on_session_creation(self, event: Callable[[], None]):
        self.__on_session_creation.remove(event)

    def add_on_session_closing(self, event: Callable[[], None]):
        self.__on_session_closing.append(event)

    def remove_on_session_closing(self, event: Callable[[], None]):
        self.__on_session_closing.remove(event)

    def add_before_conversion_to_document(self, event: Callable[[], None]):
        self.__before_conversion_to_document.append(event)

    def remove_before_conversion_to_document(self, event: Callable[[], None]):
        self.__before_conversion_to_document.remove(event)

    def add_after_conversion_to_document(self, event: Callable[[], None]):
        self.__after_conversion_to_document.append(event)

    def remove_after_conversion_to_document(self, event: Callable[[], None]):
        self.__after_conversion_to_document.remove(event)

    def add_before_conversion_to_entity(self, event: Callable[[], None]):
        self.__before_conversion_to_entity.append(event)

    def remove_before_conversion_to_entity(self, event: Callable[[], None]):
        self.__before_conversion_to_entity.remove(event)

    def add_after_conversion_to_entity(self, event: Callable[[], None]):
        self.__after_conversion_to_entity.append(event)

    def remove_after_conversion_to_entity(self, event: Callable[[], None]):
        self.__after_conversion_to_entity.remove(event)

    def register_events_for_session(self, session: InMemoryDocumentSessionOperations):
        for event in self.__before_store:
            session.add_before_store(event)

        for event in self.__after_save_changes:
            session.add_after_save_changes(event)

        for event in self.__before_delete:
            session.add_before_delete(event)

        for event in self.__before_query:
            session.add_before_query(event)

        for event in self.__before_conversion_to_document:
            session.add_before_conversion_to_document(event)

        for event in self.__after_conversion_to_document:
            session.add_after_conversion_to_document(event)

        for event in self.__before_conversion_to_entity:
            session.add_before_conversion_to_entity(event)

        for event in self.__after_conversion_to_entity:
            session.add_after_conversion_to_entity(event)

        for event in self.__on_session_closing:
            session.add_session_closing(event)

    def after_session_created(self, session: InMemoryDocumentSessionOperations):
        for event in self.__on_session_creation:
            event(SessionCreatedEventArgs(session))

    def _ensure_not_closed(self) -> None:
        if self.disposed:
            raise ValueError("The document legacy has already been disposed and cannot be used")

    def __assert_not_initialized(self, property: str) -> None:
        if self._initialized:
            raise RuntimeError(f"You cannot set '{property}' after the document legacy has been initialized.")

    def assert_initialized(self) -> None:
        if not self._initialized:
            raise RuntimeError(
                f"You cannot open a session or access the database commands before initializing the "
                f"document legacy. Did you forget calling initialize()?"
            )

    def get_effective_database(self, database: str) -> str:
        return self.get_effective_database_static(self, database)

    @staticmethod
    def get_effective_database_static(store: DocumentStoreBase, database: str) -> str:
        if database is None:
            database = store.database

        if not database.isspace():
            return database

        raise ValueError(
            "Cannot determine database to operate on. "
            "Please either specify 'database' directly as an action parameter "
            "or set the default database to operate on using 'DocumentStore.setDatabase' method. "
            "Did you forget to pass 'database' parameter?"
        )


class DocumentStore(DocumentStoreBase):
    def __init__(self, urls: Optional[Union[str, List[str]]] = None, database: Optional[str] = None):
        super(DocumentStore, self).__init__()
        self.__thread_pool_executor = ThreadPoolExecutor()
        self.urls = [urls] if isinstance(urls, str) else urls
        self.database = database
        self.__request_executors: Dict[str, Lazy[RequestExecutor]] = CaseInsensitiveDict()
        # todo: database changes
        # todo: aggressive cache
        self.__maintenance_operation_executor: Union[None, MaintenanceOperationExecutor] = None
        self.__operation_executor: Union[None, OperationExecutor] = None
        # todo: database smuggler
        self.__identifier: Union[None, str] = None
        self.__add_change_lock = threading.Lock()
        self.__database_changes = {}
        self.__after_close: List[Callable[[], None]] = []
        self.__before_close: List[Callable[[], None]] = []

    @property
    def thread_pool_executor(self):
        return self.__thread_pool_executor

    @property
    def identifier(self) -> Union[None, str]:
        if self.__identifier is not None:
            return self.__identifier

        if self.urls is None:
            return None

        if self.database is not None:
            return ",".join(self.urls) + f" (DB: {self.database})"

        return ",".join(self.urls)

    @identifier.setter
    def identifier(self, value: str):
        self.__identifier = value

    def add_after_close(self, event: Callable[[], None]):
        self.__after_close.append(event)

    def remove_after_close(self, event: Callable[[], None]):
        self.__after_close.remove(event)

    def add_before_close(self, event: Callable[[], None]):
        self.__before_close.append(event)

    def remove_before_close(self, event: Callable[[], None]):
        self.__before_close.remove(event)

    def close(self):
        for event in self.__before_close:
            event()

        # todo: evict items from cache based on changes

        while len(self.__database_changes) > 0:
            self.__database_changes.popitem()[1].close()

        if self.__multi_db_hilo is not None:
            try:
                self.__multi_db_hilo.return_unused_range()
            except Exception:
                pass  # ignore
        # todo: clear subscriptions
        self._disposed = True
        for event in self.__after_close:
            event()

        for key, lazy in self.__request_executors.items():
            if not lazy.is_value_created:
                continue

            lazy.value().close()

        self.__thread_pool_executor.shutdown()

    def open_session(
        self, database: Optional[str] = None, session_options: Optional[SessionOptions] = None
    ) -> DocumentSession:
        if not database and not session_options:
            session_options = SessionOptions()
        if not ((session_options is not None) ^ (database is not None)):
            raise ValueError("Pass either database str or session_options object")
        if database:
            session_options = SessionOptions(database=database)
        self.assert_initialized()
        self._ensure_not_closed()

        session_id = uuid.uuid4()
        session = DocumentSession(self, session_id, session_options)
        self.register_events_for_session(session)
        self.after_session_created(session)
        return session

    def get_request_executor(self, database: Optional[str] = None) -> RequestExecutor:
        self.assert_initialized()

        database = self.get_effective_database(database)

        executor = self.__request_executors.get(database, None)
        if executor:
            return executor.value()
        effective_database = database

        def __create_request_executor() -> RequestExecutor:
            request_executor = RequestExecutor.create(
                self.urls,
                effective_database,
                self.conventions,
                self.certificate_path,
                self.trust_store_path,
                self.thread_pool_executor,
            )
            # todo: register events
            return request_executor

        def __create_request_executor_for_single_node() -> RequestExecutor:
            for_single_node = RequestExecutor.create_for_single_node_with_configuration_updates(
                self.urls[0],
                effective_database,
                self.conventions,
                self.certificate_path,
                self.trust_store_path,
                self.thread_pool_executor,
            )
            # todo: register events

            return for_single_node

        executor = (
            Lazy(__create_request_executor)
            if not self.conventions.disable_topology_updates
            else Lazy(__create_request_executor_for_single_node)
        )

        self.__request_executors[database] = executor

        return executor.value()

    def execute_index(self, task: "AbstractIndexCreationTask", database: Optional[str] = None) -> None:
        self.assert_initialized()
        task.execute(self, self.conventions, database)

    def execute_indexes(self, tasks: "List[AbstractIndexCreationTask]", database: Optional[str] = None) -> None:
        self.assert_initialized()
        indexes_to_add = IndexCreation.create_indexes_to_add(tasks, self.conventions)

        self.maintenance.for_database(self.get_effective_database(database)).send(PutIndexesOperation(indexes_to_add))

    def changes(self, database=None, on_error=None, executor=None) -> DatabaseChanges:
        self.assert_initialized()
        if not database:
            database = self.database
        with self.__add_change_lock:
            if database not in self.__database_changes:
                self.__database_changes[database] = DatabaseChanges(
                    request_executor=self.get_request_executor(database),
                    database_name=database,
                    on_close=self.__on_close_change,
                    on_error=on_error,
                    executor=executor,
                )
            return self.__database_changes[database]

    def __on_close_change(self, database):
        self.__database_changes.pop(database, None)

    def set_request_timeout(self, timeout: datetime.timedelta, database: Optional[str] = None) -> Callable[[], None]:
        self.assert_initialized()

        database = self.get_effective_database(database)

        request_executor = self.get_request_executor(database)
        old_timeout = request_executor.default_timeout
        request_executor.default_timeout = timeout

        def __return_function():
            request_executor.default_timeout = old_timeout

        return __return_function

    def initialize(self) -> DocumentStore:
        if self._initialized:
            return self

        self._assert_valid_configuration()

        RequestExecutor.validate_urls(self.urls)  # todo: pass also certificate

        if self.conventions.document_id_generator is None:  # don't overwrite what the user is doing
            generator = MultiDatabaseHiLoGenerator(self)
            self.__multi_db_hilo = generator

            self.conventions.document_id_generator = generator.generate_document_id

        self.conventions.freeze()
        self._initialized = True
        return self

    # todo: changes

    # todo: aggressively cache

    def _assert_valid_configuration(self) -> None:
        if not self.urls:
            raise ValueError("Document legacy URLs cannot be empty.")

    @property
    def maintenance(self) -> MaintenanceOperationExecutor:
        self.assert_initialized()

        if self.__maintenance_operation_executor is None:
            self.__maintenance_operation_executor = MaintenanceOperationExecutor(self)

        return self.__maintenance_operation_executor

    @property
    def operations(self) -> OperationExecutor:
        if self.__operation_executor is None:
            self.__operation_executor = OperationExecutor(self)

        return self.__operation_executor


class DocumentCounters:
    @staticmethod
    def raise_not_in_session(entity):
        raise ValueError(
            repr(entity) + " is not associated with the session, cannot add counter to it. "
            "Use document_id instead or track the entity in the session."
        )

    @staticmethod
    def raise_document_already_deleted_in_session(document_id, counter_name):
        raise exceptions.InvalidOperationException(
            f"Can't increment counter {counter_name} of document {document_id}, "
            f"the document was already deleted in this session."
        )

    @staticmethod
    def raise_increment_after_delete(document_id, counter_name):
        raise exceptions.InvalidOperationException(
            f"Can't increment counter {counter_name} of document {document_id}, "
            f"there is a deferred command registered to "
            f"delete a counter with the same name."
        )

    @staticmethod
    def raise_delete_after_increment(document_id, counter_name):
        raise exceptions.InvalidOperationException(
            f"Can't delete counter {counter_name} of document {document_id}, "
            f"there is a deferred command registered to "
            f"increment a counter with the same name."
        )

    def __init__(self, session: "InMemoryDocumentSessionOperations", entity_or_document_id):
        self._session = session
        if not isinstance(entity_or_document_id, str):
            entity = self._session.documents_by_entity.get(entity_or_document_id, None)
            if not entity:
                self.raise_not_in_session(entity_or_document_id)
            entity_or_document_id = entity.get("key", None)

        if not entity_or_document_id:
            raise ValueError(entity_or_document_id)

        self._document_id = entity_or_document_id

    def increment(self, counter_name, delta=1):
        if not counter_name:
            raise ValueError("Invalid counter")

        document = self._session.documents_by_id.get(self._document_id, None)
        if document and document in self._session.deleted_entities:
            self.raise_document_already_deleted_in_session(self._document_id, self._name)

        counter_operation = CounterOperation(counter_name, CounterOperationType.increment, delta)

        command = self._session.deferred_commands_map.get(
            IdTypeAndName.create(self._document_id, CommandType.COUNTERS, None)
        )
        if command is not None:
            if command.has_delete(counter_name):
                raise self.raise_increment_after_delete(self._document_id, counter_name)
            command.counters.add_operations(counter_operation)
        else:
            command = CountersBatchCommandData(self._document_id, counter_operation)
            self._session.deferred_commands_map[
                IdTypeAndName.create(self._document_id, CommandType.COUNTERS, None)
            ] = command
            self._session.defer(command)

    def delete(self, counter_name):
        if not counter_name:
            raise ValueError("None or empty counter is invalid")

        document = self._session.documents_by_id.get(self._document_id, None)
        if document and document in self._session.deleted_entities:
            return

        counter_operation = CounterOperation(counter_name, CounterOperationType.delete)
        command: CountersBatchCommandData = self._session.counters_defer_commands.get(self._document_id, None)
        if command:
            if command.has_delete(counter_name):
                return
            if command.has_increment(counter_name):
                raise self.raise_delete_after_increment(self._document_id, counter_name)

            command.counters.add_operations(counter_operation)

        command = CountersBatchCommandData(self._document_id, counter_operations=counter_operation)
        self._session.counters_defer_commands[self._document_id] = command
        self._session.defer(command)

        cache = self._session.counters_by_document_id.get(self._document_id, None)
        if cache:
            cache[1].pop(counter_name, None)
        cache = self._session.included_counters_by_document_id.get(self._document_id, None)
        if cache:
            cache[1].pop(counter_name, None)

    def get_all(self):
        """
        Get all the counters for the document
        """
        cache = self._session.counters_by_document_id.get(self._document_id, None)
        if not cache:
            cache = [False, CaseInsensitiveDict()]
        missing_counters = not cache[0]

        document = self._session.advanced.get_document_by_id(self._document_id)
        if document:
            metadata_counters = document["metadata"].get(constants.Documents.Metadata.COUNTERS, None)
            if not metadata_counters:
                missing_counters = False
            elif len(cache[1]) >= len(metadata_counters):
                missing_counters = False
                for c in metadata_counters:
                    if str(c) in cache[1]:
                        continue
                    missing_counters = True
                    break

        if missing_counters:
            self._session.increment_requests_count()
            details = self._session.advanced.document_store.operations.send(
                GetCountersOperation(document_id=self._document_id)
            )
            cache[1].clear()
            for counter_detail in details["Counters"]:
                cache[1][counter_detail["CounterName"]] = counter_detail["TotalValue"]

        cache[0] = True
        if not self._session.no_tracking:
            self._session.counters_by_document_id[self._document_id] = cache
        return cache[1]

    def get(self, *counter_names: str):
        """
        Get the counter by counter name
        """

        cache = self._session.counters_by_document_id.get(self._document_id, None)
        if len(counter_names) == 1:
            value = None
            counter = counter_names[0]
            if cache:
                value = cache[1].get(counter, None)
                if value:
                    return value
            else:
                cache = [False, CaseInsensitiveDict()]
            document = self._session.advanced.get_document_by_id(self._document_id)
            metadata_has_counter_name = False
            if document:
                metadata_counters = document["metadata"].get(constants.Documents.Metadata.COUNTERS)
                if metadata_counters:
                    metadata_has_counter_name = counter.lower() in list(map(str.lower, metadata_counters))
            if (document is None and not cache[0]) or metadata_has_counter_name:
                self._session.increment_requests_count()
                details = self._session.advanced.document_store.operations.send(
                    GetCountersOperation(document_id=self._document_id, counters=list(counter_names))
                )
                counter_detail = details.get("Counters", None)[0]
                value = counter_detail["TotalValue"] if counter_detail else None

            cache[1].update({counter: value})
            if self._session.no_tracking:
                self._session.counters_by_document_id.update({self._document_id: cache})
            return value

        if cache is None:
            cache = [False, CaseInsensitiveDict()]

        metadata_counters = None
        document = self._session.advanced.get_document_by_id(self._document_id)

        if document:
            metadata_counters = document["metadata"].get(constants.Documents.Metadata.COUNTERS)

        result = {}

        for counter in counter_names:
            has_counter = counter in cache[1]
            val = cache[1].get(counter, None)
            not_in_metadata = True

            if document and metadata_counters:
                for metadata_counter in metadata_counters:
                    if str(metadata_counter).lower() == counter.lower():
                        not_in_metadata = False

            if has_counter or cache[0] or document and not_in_metadata:
                result[counter] = val
                continue

            result.clear()
            self._session.increment_requests_count()
            details = self._session.advanced.document_store.send(
                GetCountersOperation(document_id=self._document_id, counters=list(counter_names))
            )

            for counter_detail in details["Counters"]:
                if not counter_detail:
                    continue
                cache[1][counter_detail["CounterName"]] = counter_detail["TotalValue"]
                result[counter_detail["CounterName"]] = counter_detail["TotalValue"]
            break

        if not self._session.no_tracking:
            self._session.counters_by_document_id[self._document_id] = cache

        return result if len(counter_names) > 1 else result[counter_names[0]]

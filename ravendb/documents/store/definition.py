from __future__ import annotations
import threading
import datetime
import uuid
from abc import abstractmethod
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, Union, Optional, TypeVar, List, Dict, TYPE_CHECKING

from ravendb.changes.database_changes import DatabaseChanges
from ravendb.documents.bulk_insert_operation import BulkInsertOperation, BulkInsertOptions
from ravendb.documents.indexes.index_creation import IndexCreation
from ravendb.documents.operations.executor import MaintenanceOperationExecutor, OperationExecutor
from ravendb.documents.operations.indexes import PutIndexesOperation
from ravendb.documents.session.event_args import (
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
from ravendb.documents.store.lazy import Lazy
from ravendb.documents.session.document_session import DocumentSession
from ravendb.documents.session.document_session_operations.in_memory_document_session_operations import (
    InMemoryDocumentSessionOperations,
)
from ravendb.documents.session.misc import SessionOptions
from ravendb.documents.subscriptions.document_subscriptions import DocumentSubscriptions
from ravendb.documents.time_series import TimeSeriesOperations
from ravendb.http.request_executor import RequestExecutor
from ravendb.documents.identity.hilo import MultiDatabaseHiLoGenerator
from ravendb.http.topology import Topology
from ravendb.tools.utils import CaseInsensitiveDict
from ravendb.documents.conventions import DocumentConventions

T = TypeVar("T")

if TYPE_CHECKING:
    from ravendb.documents.indexes.abstract_index_creation_tasks import AbstractIndexCreationTask
    from ravendb.documents.smuggler.database_smuggler import DatabaseSmuggler
    from ravendb.http.misc import AggressiveCacheMode, AggressiveCacheOptions


class DocumentStoreBase:
    def __init__(self):
        self.__conventions = None
        self._initialized: bool = False

        self.__certificate_pem_path: Optional[str] = None
        self.__trust_store_path: Optional[str] = None

        self._urls: List[str] = []
        self._database: Optional[str] = None
        self._disposed: Optional[bool] = None

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

        self.__on_before_request: List[Callable[[BeforeRequestEventArgs], None]] = []
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
    def certificate_pem_path(self) -> str:
        return self.__certificate_pem_path

    @certificate_pem_path.setter
    def certificate_pem_path(self, value: str):
        self.__assert_not_initialized("certificate_url")
        if value.endswith("pfx"):
            raise ValueError("Invalid certificate format. " "Please use .pem file.")
        with open(value) as file:
            content = file.read()
            if "BEGIN CERTIFICATE" not in content:
                raise ValueError(
                    f"Invalid file. File stored under the path '{value}' isn't valid .pem certificate. "
                    f"BEGIN CERTIFICATE header wasn't found."
                )
            if "PRIVATE KEY" not in content:
                raise ValueError(
                    f"Invalid file. File stored under the path '{value}' isn't valid .pem certificate. "
                    f"PRIVATE KEY header wasn't found."
                )
        self.__certificate_pem_path = value

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

    # todo: aggressive_caching

    # todo: time_series

    @abstractmethod
    def open_session(self, database: Optional[str] = None, session_options: Optional = None):
        pass

    def add_before_store(self, event: Callable[[BeforeStoreEventArgs], None]):
        self.__before_store.append(event)

    def remove_before_store(self, event: Callable[[BeforeStoreEventArgs], None]):
        self.__before_store.remove(event)

    def add_after_save_changes(self, event: Callable[[AfterSaveChangesEventArgs], None]):
        self.__after_save_changes.append(event)

    def remove_after_save_changes(self, event: Callable[[AfterSaveChangesEventArgs], None]):
        self.__after_save_changes.remove(event)

    def add_before_delete(self, event: Callable[[BeforeDeleteEventArgs], None]):
        self.__before_delete.append(event)

    def remove_before_delete(self, event: Callable[[BeforeDeleteEventArgs], None]):
        self.__before_delete.remove(event)

    def add_before_query(self, event: Callable[[BeforeQueryEventArgs], None]):
        self.__before_query.append(event)

    def remove_before_query(self, event: Callable[[BeforeQueryEventArgs], None]):
        self.__before_query.remove(event)

    def add_on_session_creation(self, event: Callable[[SessionCreatedEventArgs], None]):
        self.__on_session_creation.append(event)

    def remove_on_session_creation(self, event: Callable[[SessionCreatedEventArgs], None]):
        self.__on_session_creation.remove(event)

    def add_on_session_closing(self, event: Callable[[SessionClosingEventArgs], None]):
        self.__on_session_closing.append(event)

    def remove_on_session_closing(self, event: Callable[[SessionClosingEventArgs], None]):
        self.__on_session_closing.remove(event)

    def add_before_conversion_to_document(self, event: Callable[[BeforeConversionToDocumentEventArgs], None]):
        self.__before_conversion_to_document.append(event)

    def remove_before_conversion_to_document(self, event: Callable[[BeforeConversionToDocumentEventArgs], None]):
        self.__before_conversion_to_document.remove(event)

    def add_after_conversion_to_document(self, event: Callable[[AfterConversionToDocumentEventArgs], None]):
        self.__after_conversion_to_document.append(event)

    def remove_after_conversion_to_document(self, event: Callable[[AfterConversionToDocumentEventArgs], None]):
        self.__after_conversion_to_document.remove(event)

    def add_before_conversion_to_entity(self, event: Callable[[BeforeConversionToEntityEventArgs], None]):
        self.__before_conversion_to_entity.append(event)

    def remove_before_conversion_to_entity(self, event: Callable[[BeforeConversionToEntityEventArgs], None]):
        self.__before_conversion_to_entity.remove(event)

    def add_after_conversion_to_entity(self, event: Callable[[AfterConversionToEntityEventArgs], None]):
        self.__after_conversion_to_entity.append(event)

    def remove_after_conversion_to_entity(self, event: Callable[[AfterConversionToEntityEventArgs], None]):
        self.__after_conversion_to_entity.remove(event)

    def add_on_before_request(self, event: Callable[[BeforeRequestEventArgs], None]):
        self.__on_before_request.append(event)

    def remove_on_before_request(self, event: Callable[[BeforeRequestEventArgs], None]):
        self.__on_before_request.remove(event)

    def add_on_succeed_request(self, event: Callable[[SucceedRequestEventArgs], None]):
        self.__on_succeed_request.append(event)

    def remove_on_succeed_request(self, event: Callable[[SucceedRequestEventArgs], None]):
        self.__on_succeed_request.remove(event)

    def add_on_failed_request(self, event: Callable[[FailedRequestEventArgs], None]):
        self.__on_failed_request.append(event)

    def remove_on_failed_request(self, event: Callable[[FailedRequestEventArgs], None]):
        self.__on_failed_request.remove(event)

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

    def register_events_for_request_executor(self, request_executor: RequestExecutor):
        for event in self.__on_before_request:
            request_executor.add_on_before_request(event)
        for event in self.__on_failed_request:
            request_executor.add_on_failed_request(event)
        for event in self.__on_succeed_request:
            request_executor.add_on_succeed_request(event)
        for event in self.__on_topology_updated:
            request_executor.add_on_topology_updated(event)

    def after_session_created(self, session: InMemoryDocumentSessionOperations):
        for event in self.__on_session_creation:
            event(SessionCreatedEventArgs(session))

    def _ensure_not_closed(self) -> None:
        if self.disposed:
            raise ValueError("The document store has already been disposed and cannot be used")

    def __assert_not_initialized(self, property: str) -> None:
        if self._initialized:
            raise RuntimeError(f"You cannot set '{property}' after the document store has been initialized.")

    def assert_initialized(self) -> None:
        if not self._initialized:
            raise RuntimeError(
                f"You cannot open a session or access the database commands before initializing the "
                f"document store. Did you forget calling initialize()?"
            )

    def get_effective_database(self, database: str) -> str:
        return self.get_effective_database_static(self, database)

    @staticmethod
    def get_effective_database_static(store: DocumentStoreBase, database: str) -> str:
        if database is None:
            database = store.database

        if database and not database.isspace():
            return database

        raise ValueError(
            "Cannot determine database to operate on. "
            "Please either specify 'database' directly as an action parameter "
            "or set the default database to operate on using 'DocumentStore.database'. "
            "Did you forget to pass 'database' parameter?"
        )


class DocumentStore(DocumentStoreBase):
    def __init__(self, urls: Union[str, List[str]] = None, database: Optional[str] = None):
        super(DocumentStore, self).__init__()
        self.__subscriptions = DocumentSubscriptions(self)
        self.__thread_pool_executor = ThreadPoolExecutor()
        self.urls = [urls] if isinstance(urls, str) else urls
        self.database = database
        self.__request_executors: Dict[str, Lazy[RequestExecutor]] = CaseInsensitiveDict()
        self.__aggressive_cache_changes: Dict[str, "DocumentStore._AggressiveCacheInvalidator"] = {}
        self.__maintenance_operation_executor: Optional[MaintenanceOperationExecutor] = None
        self.__operation_executor: Optional[OperationExecutor] = None
        self.__smuggler: Optional["DatabaseSmuggler"] = None
        self.__multi_db_hilo: Optional[MultiDatabaseHiLoGenerator] = None
        self.__identifier: Optional[str] = None
        self.__add_change_lock = threading.Lock()
        self.__database_changes: Dict[str, DatabaseChanges] = {}
        self.__after_close: List[Callable[[], None]] = []
        self.__before_close: List[Callable[[], None]] = []
        self.__time_series_operation: Optional[TimeSeriesOperations] = None
        self.__ai_operations = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    @property
    def hilo_id_generator(self) -> Optional[MultiDatabaseHiLoGenerator]:
        return self.__multi_db_hilo

    @property
    def thread_pool_executor(self) -> ThreadPoolExecutor:
        return self.__thread_pool_executor

    @property
    def subscriptions(self) -> DocumentSubscriptions:
        return self.__subscriptions

    @property
    def identifier(self) -> Optional[str]:
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
        if self._disposed:
            return
        for event in self.__before_close:
            event()

        for cache_invalidator in list(self.__aggressive_cache_changes.values()):
            cache_invalidator.close()
        self.__aggressive_cache_changes.clear()

        while len(self.__database_changes) > 0:
            self.__database_changes.popitem()[1].close()

        if self.__multi_db_hilo is not None:
            try:
                self.__multi_db_hilo.return_unused_range()
            except Exception:
                pass  # ignore

        if self.subscriptions is not None:
            self.subscriptions.close()

        for event in self.__after_close:
            event()

        for key, lazy in self.__request_executors.items():
            if not lazy.is_value_created:
                continue

            lazy.value.close()

        self.__thread_pool_executor.shutdown()
        self._disposed = True

    def open_session(
        self, database: Optional[str] = None, session_options: Optional[SessionOptions] = None
    ) -> DocumentSession:
        if not session_options:
            session_options = SessionOptions()
            session_options.database = database
            session_options.disable_atomic_document_writes_in_cluster_wide_transaction = (
                self.conventions.disable_atomic_document_writes_in_cluster_wide_transaction
            )

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
            return executor.value
        effective_database = database

        def __create_request_executor() -> RequestExecutor:
            request_executor = RequestExecutor.create(
                self.urls,
                effective_database,
                self.conventions,
                self.certificate_pem_path,
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
                self.certificate_pem_path,
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

        return executor.value

    def execute_index(self, task: "AbstractIndexCreationTask", database: Optional[str] = None) -> None:
        self.assert_initialized()
        task.execute(self, self.conventions, database)

    def execute_indexes(self, tasks: "List[AbstractIndexCreationTask]", database: Optional[str] = None) -> None:
        self.assert_initialized()
        indexes_to_add = IndexCreation.create_indexes_to_add(tasks, self.conventions)

        self.maintenance.for_database(self.get_effective_database(database)).send(PutIndexesOperation(*indexes_to_add))

    def changes(self, database=None, on_error=None, executor=None) -> DatabaseChanges:  # todo: sync with java
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

    def __on_close_change(self, database_name: str):
        self.__database_changes.pop(database_name, None)

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

    def aggressively_cache_for(
        self,
        cache_duration: datetime.timedelta,
        database: Optional[str] = None,
        mode: Optional["AggressiveCacheMode"] = None,
    ) -> "DocumentStore._AggressiveCacheOverrideScope":
        from ravendb.http.misc import AggressiveCacheMode

        if mode is None:
            mode = AggressiveCacheMode.TRACK_CHANGES
        context = self._set_aggressive_cache(cache_duration, mode, database)
        return self._finalize_aggressive_cache(context, mode, database)

    def _set_aggressive_cache(
        self,
        cache_duration: datetime.timedelta,
        mode: "AggressiveCacheMode",
        database: Optional[str] = None,
    ) -> "DocumentStore._AggressiveCacheOverrideScope":
        from ravendb.http.misc import AggressiveCacheOptions

        self.assert_initialized()
        database = self.get_effective_database(database)
        request_executor = self.get_request_executor(database)
        options = AggressiveCacheOptions(cache_duration, mode)
        return DocumentStore._AggressiveCacheOverrideScope(request_executor, options)

    def _finalize_aggressive_cache(
        self,
        context: "DocumentStore._AggressiveCacheOverrideScope",
        mode: "AggressiveCacheMode",
        database: Optional[str] = None,
    ) -> "DocumentStore._AggressiveCacheOverrideScope":
        from ravendb.http.misc import AggressiveCacheMode

        try:
            if mode != AggressiveCacheMode.DO_NOT_TRACK_CHANGES:
                database = self.get_effective_database(database)
                self._listen_to_changes_and_update_cache(database)
            return context
        except Exception:
            context.__enter__()
            context.__exit__(None, None, None)
            raise

    def _listen_to_changes_and_update_cache(self, database: str) -> None:
        if database in self.__aggressive_cache_changes:
            return
        # This lock achieves ConcurrentDict-like behavior
        cache_invalidator = DocumentStore._AggressiveCacheInvalidator(self, database)
        with self.__add_change_lock:
            if database not in self.__aggressive_cache_changes:
                self.__aggressive_cache_changes[database] = cache_invalidator
                cache_invalidator.ensure_connected()
            else:
                cache_invalidator.close()

    def disable_aggressive_caching(
        self, database: Optional[str] = None
    ) -> "DocumentStore._AggressiveCacheOverrideScope":
        self.assert_initialized()
        database = self.get_effective_database(database)
        request_executor = self.get_request_executor(database)
        return DocumentStore._AggressiveCacheOverrideScope(request_executor)

    class _AggressiveCacheOverrideScope:
        def __init__(self, request_executor: RequestExecutor, options: AggressiveCacheOptions = None):
            self._request_executor = request_executor
            self._options = options
            self._old_options: Optional[AggressiveCacheOptions] = None

        def __enter__(self):
            self._old_options = self._request_executor.aggressive_caching
            self._request_executor.aggressive_caching = self._options
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            self._request_executor.aggressive_caching = self._old_options

    class _AggressiveCacheInvalidator:
        """Subscribes to document/index changes and invalidates the request executor's aggressive cache."""

        def __init__(self, store: "DocumentStore", database: str):
            from ravendb.changes.observers import ActionObserver
            from ravendb.changes.types import DocumentChangeType, IndexChangeTypes

            self._request_executor = store.get_request_executor(database)
            self._changes = store.changes(database)
            self._unsubscribers: List[Callable[[], None]] = []

            # Capture by reference so the lambdas below always see the live cache object,
            # even if RequestExecutor.cache is replaced. (It isn't today, but be explicit.)
            cache_ref = self._request_executor.cache

            def _invalidate() -> None:
                cache_ref.generation += 1

            def on_document_change(change) -> None:
                # Only Put and Delete affect query results; ConflictResolved etc. do not.
                if change.type_of_change in (DocumentChangeType.PUT, DocumentChangeType.DELETE):
                    _invalidate()

            def on_index_change(change) -> None:
                # BatchCompleted means new index results are available; IndexRemoved means
                # stale queries might have been using it.
                if change.type_of_change in (IndexChangeTypes.BATCH_COMPLETED, IndexChangeTypes.INDEX_REMOVED):
                    _invalidate()

            # subscribe_with_observer (not subscribe) so we can attach an on_error callback.
            # subscribe() creates an ActionObserver with no on_error, which means a WebSocket
            # disconnect silently swallows the error — cache.generation is never bumped and
            # the aggressive cache serves stale data indefinitely after the connection dies.
            self._unsubscribers.append(
                self._changes.for_all_documents().subscribe_with_observer(
                    ActionObserver(on_next=on_document_change, on_error=lambda e: _invalidate())
                )
            )
            self._unsubscribers.append(
                self._changes.for_all_indexes().subscribe_with_observer(
                    ActionObserver(on_next=on_index_change, on_error=lambda e: _invalidate())
                )
            )

        def ensure_connected(self) -> None:
            self._changes.ensure_connected_now()

        def close(self) -> None:
            for unsub in self._unsubscribers:
                try:
                    unsub()
                except Exception:
                    pass
            self._unsubscribers.clear()

    def bulk_insert(self, database_name: str = None, options: BulkInsertOptions = None) -> BulkInsertOperation:
        self.assert_initialized()
        return BulkInsertOperation(self.get_effective_database(database_name), self, options)

    def _assert_valid_configuration(self) -> None:
        if not self.urls:
            raise ValueError("Document URLs cannot be empty.")

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

    @property
    def smuggler(self) -> "DatabaseSmuggler":
        self.assert_initialized()

        if self.__smuggler is None:
            from ravendb.documents.smuggler.database_smuggler import DatabaseSmuggler

            self.__smuggler = DatabaseSmuggler(self)

        return self.__smuggler

    @property
    def time_series(self) -> TimeSeriesOperations:
        if self.__time_series_operation is None:
            self.__time_series_operation = TimeSeriesOperations(self)

        return self.__time_series_operation

    @property
    def ai(self):
        if self.__ai_operations is None:
            from ravendb.documents.ai import AiOperations

            self.__ai_operations = AiOperations(self)

        return self.__ai_operations

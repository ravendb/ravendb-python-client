from __future__ import annotations
import threading
import datetime
import uuid
from abc import abstractmethod
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, Any, Union, Optional, TypeVar, Generic, TYPE_CHECKING, List, Dict

from pyravendb.documents.session.document_session import DocumentSession
from pyravendb.documents.session.in_memory_document_session_operations import InMemoryDocumentSessionOperations
from pyravendb.documents.session import SessionOptions
from pyravendb.http.request_executor import RequestExecutor
from pyravendb.tools.utils import CaseInsensitiveDict
from pyravendb.data.document_conventions import DocumentConventions

import pyravendb.documents.operations as documents_operations

T = TypeVar("T")

import pyravendb.documents.identity

if TYPE_CHECKING:
    from pyravendb.documents.commands.batches import CommandType


class Lazy(Generic[T]):
    def __init__(self, value_factory: Callable[[], Any]):
        self.__value_created = False  # todo: check if it isn't cached - volatile in java
        self.__value_factory = value_factory
        self.__value: Union[None, T] = None
        self.__value_lock = threading.Lock()

    @property
    def is_value_created(self) -> bool:
        return self.__value_created

    def value(self) -> T:  # double check locking
        if self.__value_created:
            return self.__value
        with self.__value_lock:
            if not self.__value_created:
                self.__value = self.__value_factory()
                self.__value_created = True
        return self.__value


class IdTypeAndName:
    def __init__(self, key: str, command_type: CommandType, name: str):
        self.key = key
        self.command_type = command_type
        self.name = name

    def __eq__(self, other):
        if self == other:
            return True
        if other is None or type(self) != type(other):
            return False
        other: IdTypeAndName
        if self.key != other.key if self.key else other.key:
            return False
        if self.command_type != other.command_type:
            return False
        return self.name == other.name if self.name is not None else other.name is None

    def __hash__(self):
        return id(self)

    @staticmethod
    def create(key: str, command_type: CommandType, name: str) -> IdTypeAndName:
        return IdTypeAndName(key, command_type, name)


class DocumentStoreBase:
    def __init__(self):

        self.__conventions = None
        self._initialized: bool = False
        self._urls: List[str] = []
        self._database: Union[None, str] = None
        self._disposed: Union[None, bool] = None
        # todo: cryptography & ceritifiactes

        # todo: events
        self.on_before_store: List[Callable[[InMemoryDocumentSessionOperations, str, object], None]] = []

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

    @abstractmethod
    def close(self):
        pass

    @abstractmethod
    def maintenance(self) -> documents_operations.MaintenanceOperationExecutor:
        pass

    @abstractmethod
    def operations(self) -> documents_operations.OperationExecutor:
        pass

    # todo: changes

    # todo: aggressive_caching

    # todo: execute_indexes

    # todo: time_series

    # todo: bulk_insert

    @abstractmethod
    def open_session(self, database: Optional[str] = None, session_options: Optional = None):
        pass

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
        # todo: hilo
        self.__maintenance_operation_executor: Union[None, documents_operations.MaintenanceOperationExecutor] = None
        self.__operation_executor: Union[None, documents_operations.OperationExecutor] = None
        # todo: database smuggler
        self.__identifier: Union[None, str] = None

    @property
    def thread_pool_executor(self):
        return self.__thread_pool_executor

    @property
    def identifier(self) -> str:
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

    def close(self):
        # todo: event on before close
        # todo: evict items from cache based on changes
        # todo: clear database changes
        # todo: clear hilo
        # todo: clear subscriptions
        self._disposed = True
        # todo: event after close
        # todo: shutdown request executors

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
        # todo: register all events
        # todo: after session creation
        return session

    def get_request_executor(self, database: Optional[str] = None) -> RequestExecutor:
        self.assert_initialized()

        database = self.get_effective_database(database)

        executor = self.__request_executors.get(database, None)
        if executor:
            return executor.value()
        effective_database = database

        def __create_request_executor() -> RequestExecutor:
            # todo : push certificate here
            request_executor = RequestExecutor.create(self.urls, effective_database, self.conventions)
            # todo: register events
            return request_executor

        def __create_request_executor_for_single_node() -> RequestExecutor:
            for_single_node = RequestExecutor.create_for_single_node_with_configuration_updates(
                self.urls[0], effective_database, None, self.conventions
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
            generator = pyravendb.documents.identity.MultiDatabaseHiLoGenerator(self)
            self.__multi_db_hilo = generator

            self.conventions.document_id_generator = generator.generate_document_id

        self.conventions.freeze()
        self._initialized = True
        return self

    # todo: changes

    # todo: aggressively cache

    def _assert_valid_configuration(self) -> None:
        if not self.urls:
            raise ValueError("Document store URLs cannot be empty.")

    @property
    def maintenance(self) -> documents_operations.MaintenanceOperationExecutor:
        self.assert_initialized()

        if self.__maintenance_operation_executor is None:
            self.__maintenance_operation_executor = documents_operations.MaintenanceOperationExecutor(self)

        return self.__maintenance_operation_executor

    @property
    def operations(self) -> documents_operations.OperationExecutor:
        if self.__operation_executor is None:
            self.__operation_executor = documents_operations.OperationExecutor(self)

        return self.__operation_executor

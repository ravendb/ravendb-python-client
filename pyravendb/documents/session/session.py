from __future__ import annotations
import datetime
from enum import Enum
from typing import Union

from pyravendb.connection.requests_executor import RequestsExecutor
from pyravendb.documents.session.in_memory_document_session_operations import InMemoryDocumentSessionOperations
from pyravendb.documents.session.transaction_mode import TransactionMode
from pyravendb.raven_operations.query_operation import QueryOperation
from pyravendb.store.document_store import DocumentStore
from pyravendb.store.session_query import Query


class ForceRevisionStrategy(Enum):
    NONE = "None"
    BEFORE = "Before"

    def __str__(self):
        return self.value


class SessionOptions:
    def __init__(
        self,
        database: str,
        no_tracking: bool,
        no_caching: bool,
        request_executor: RequestsExecutor,
        transaction_mode: TransactionMode,
    ):
        self.database = database
        self.no_tracking = no_tracking
        self.no_caching = no_caching
        self.request_executor = request_executor
        self.transaction_mode = transaction_mode


class SessionInfo:
    def __init__(
        self, session: InMemoryDocumentSessionOperations, options: SessionOptions, document_store: DocumentStore
    ):
        if not document_store:
            raise ValueError("DocumentStore cannot be None")
        if not session:
            raise ValueError("Session cannot be None")
        self.session = session
        self.__load_balancer_context_seed = session.request_executor.conventions.load_balancer_context_seed
        # todo: can use load balance behavior
        self.options = options
        self.document_store = document_store
        self.no_caching = options.no_caching

        self.last_cluster_transaction_index: Union[None, int] = None


class DocumentQueryCustomization:
    def __init__(self, query: Query):
        self.query = query
        self.query_operation: QueryOperation = None


class DocumentsChanges:
    class ChangeType(Enum):
        DOCUMENT_DELETED = "document_deleted"
        DOCUMENT_ADDED = "document_added"
        FIELD_CHANGED = "field_changed"
        NEW_FIELD = "new_field"
        REMOVED_FIELD = "removed_field"
        ARRAY_VALUE_CHANGED = "array_value_changed"
        ARRAY_VALUE_ADDED = "array_value_added"
        ARRAY_VALUE_REMOVED = "array_value_removed"

        def __str__(self):
            return self.value

    def __init__(
        self,
        field_old_value: object,
        field_new_value: object,
        change: ChangeType,
        field_name: str = None,
        field_path: str = None,
    ):
        self.field_old_value = field_old_value
        self.field_new_value = field_new_value
        self.change = change
        self.field_name = field_name
        self.field_path = field_path

    @property
    def field_full_name(self) -> str:
        return self.field_name if not self.field_path else f"{self.field_path}.{self.field_name}"


class ResponseTimeInformation:
    class ResponseTimeItem:
        def __init__(self, url: str = None, duration: datetime.timedelta = None):
            self.url = url
            self.duration = duration

    def __init__(
        self,
        total_server_duration: datetime.timedelta = datetime.timedelta.min,
        total_client_duration: datetime.timedelta = datetime.timedelta.min,
        duration_breakdown: list[ResponseTimeItem] = None,
    ):
        self.total_server_duration = total_server_duration
        self.total_client_duration = total_client_duration
        self.duration_breakdown = duration_breakdown if duration_breakdown is not None else []

    def compute_server_total(self):
        self.total_server_duration = sum(map(lambda x: x.duration, self.duration_breakdown))


class JavaScriptArray:
    def __init__(self, suffix: int, path_to_array: str):
        self.__suffix = suffix
        self.__path_to_array = path_to_array
        self.__arg_counter = 0
        self.__script_lines = []
        self.__parameters: dict[str, object] = {}

    @property
    def script(self) -> str:
        return "\r".join(self.__script_lines)

    @property
    def parameters(self) -> dict[str, object]:
        return self.__parameters

    def __get_next_argument_name(self) -> str:
        self.__arg_counter += 1
        return f"val_{self.__arg_counter-1}_{self.__suffix}"

    def add(self, *u) -> JavaScriptArray:
        def __func(value) -> str:
            argument_name = self.__get_next_argument_name()
            self.__parameters[argument_name] = value
            return f"args.{argument_name}"

        args = list(map(__func, u))
        self.__script_lines.append(f"this.{self.__path_to_array}.push({args});")
        return self

    def remove_at(self, index: int) -> JavaScriptArray:
        argument_name = self.__get_next_argument_name()

        self.__script_lines.append(f"this.{self.__path_to_array}.splice(args.{argument_name}, 1);")
        self.__parameters[argument_name] = index

        return self


class JavaScriptMap:
    def __init__(self, suffix: int, path_to_map: str):
        self.__suffix = suffix
        self.__path_to_map = path_to_map
        self.__arg_counter = 0
        self.__script_lines = []
        self.__parameters: dict[str, object] = {}

    @property
    def script(self) -> str:
        return "\r".join(self.__script_lines)

    @property
    def parameters(self) -> dict[str, object]:
        return self.__parameters

    def __get_next_argument_name(self) -> str:
        self.__arg_counter += 1
        return f"val_{self.__arg_counter - 1}_{self.__suffix}"

    def put(self, key, value) -> JavaScriptMap:
        argument_name = self.__get_next_argument_name()

        self.__script_lines.append(f"this.{self.__path_to_map}.{key} = args.{argument_name};")
        self.parameters[argument_name] = value
        return self

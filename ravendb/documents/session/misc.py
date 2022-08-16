from __future__ import annotations
import datetime
import hashlib
import threading
from abc import ABC
from enum import Enum
from typing import Union, Optional, TYPE_CHECKING, List, Dict

from ravendb.http.misc import LoadBalanceBehavior

if TYPE_CHECKING:
    from ravendb.http.request_executor import RequestExecutor
    from ravendb.documents.session.query import Query
    from ravendb.documents.session.operations.query import QueryOperation
    from ravendb.documents.session.in_memory_document_session_operations import InMemoryDocumentSessionOperations
    from ravendb.documents import DocumentStore


class TransactionMode(Enum):
    SINGLE_NODE = "single_node"
    CLUSTER_WIDE = "cluster_wide"

    def __str__(self):
        return self.value


class ForceRevisionStrategy(Enum):
    NONE = "None"
    BEFORE = "Before"

    def __str__(self):
        return self.value


class SessionInfo:
    __client_session_id_counter = threading.local()
    __client_session_id_counter.counter = 0

    def __init__(
        self, session: InMemoryDocumentSessionOperations, options: SessionOptions, document_store: DocumentStore
    ):
        if not document_store:
            raise ValueError("DocumentStore cannot be None")
        if not session:
            raise ValueError("Session cannot be None")
        self.__session = session
        self.__session_id: Union[None, int] = None
        self.__session_id_used: Union[None, bool] = None
        self.__load_balancer_context_seed = session._request_executor.conventions.load_balancer_context_seed
        self.__can_use_load_balance_behavior = (
            session.conventions.load_balance_behavior == LoadBalanceBehavior.USE_SESSION_CONTEXT
            and session.conventions.load_balancer_per_session_context_selector is not None
        )
        self.document_store = document_store
        self.no_caching = options.no_caching

        self.last_cluster_transaction_index: Union[None, int] = None

    @property
    def can_use_load_balance_behavior(self) -> bool:
        return self.__can_use_load_balance_behavior

    @property
    def session_id(self) -> int:
        if self.__session_id is None:
            context = None
            selector = self.__session.conventions.load_balancer_per_session_context_selector
            if selector is not None:
                context = selector(self.__session.database_name)
            self.__set_context_internal(context)
        self.__session_id_used = True
        return self.__session_id

    def __set_context_internal(self, session_key: str) -> None:
        if self.__session_id_used:
            raise RuntimeError(
                "Unable to set the session context after it has already been used. "
                "The session context can only be modified before it is utilized."
            )

        if session_key is None:
            v = self.__client_session_id_counter.counter
            self.__session_id = v
            v += 1
            self.__client_session_id_counter = v
        else:
            self.__session_id = int(hashlib.md5(bytes(session_key)).digest().decode("utf-8"))


class SessionOptions:
    def __init__(
        self,
        database: Optional[str] = None,
        no_tracking: Optional[bool] = None,
        no_caching: Optional[bool] = None,
        request_executor: Optional[RequestExecutor] = None,
        transaction_mode: Optional[TransactionMode] = None,
        disable_atomic_document_writes_in_cluster_wide_transaction: Optional[bool] = None,
    ):
        self.database = database
        self.no_tracking = no_tracking
        self.no_caching = no_caching
        self.request_executor = request_executor
        self.transaction_mode = transaction_mode
        self.disable_atomic_document_writes_in_cluster_wide_transaction = (
            disable_atomic_document_writes_in_cluster_wide_transaction
        )


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
        duration_breakdown: List[ResponseTimeItem] = None,
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
        self.__parameters: Dict[str, object] = {}

    @property
    def script(self) -> str:
        return "\r".join(self.__script_lines)

    @property
    def parameters(self) -> Dict[str, object]:
        return self.__parameters

    def __get_next_argument_name(self) -> str:
        self.__arg_counter += 1
        return f"val_{self.__arg_counter-1}_{self.__suffix}"

    def add(self, *u) -> JavaScriptArray:
        def __func(value) -> str:
            argument_name = self.__get_next_argument_name()
            self.__parameters[argument_name] = value
            return f"args.{argument_name}"

        args = ",".join(list(map(__func, u)))
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
        self.__parameters: Dict[str, object] = {}

    @property
    def script(self) -> str:
        return "\r".join(self.__script_lines)

    @property
    def parameters(self) -> Dict[str, object]:
        return self.__parameters

    def __get_next_argument_name(self) -> str:
        self.__arg_counter += 1
        return f"val_{self.__arg_counter - 1}_{self.__suffix}"

    def put(self, key, value) -> JavaScriptMap:
        argument_name = self.__get_next_argument_name()

        self.__script_lines.append(f"this.{self.__path_to_map}.{key} = args.{argument_name};")
        self.parameters[argument_name] = value
        return self


class MethodCall(ABC):
    def __init__(self, args: List[object] = None, access_path: str = None):
        self.args = args
        self.access_path = access_path


class CmpXchg(MethodCall):
    @classmethod
    def value(cls, key: str) -> CmpXchg:
        cmp_xchg = cls()
        cmp_xchg.args = [key]

        return cmp_xchg


class OrderingType(Enum):
    STRING = 0
    LONG = " AS long"
    FLOAT = " AS double"
    ALPHA_NUMERIC = " AS alphaNumeric"

    def __str__(self):
        return self.value

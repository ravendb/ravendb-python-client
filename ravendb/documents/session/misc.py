from __future__ import annotations
import datetime
import hashlib
import threading
from abc import ABC
from enum import Enum
from typing import Union, Optional, TYPE_CHECKING, List, Dict, Generic, TypeVar

from ravendb.http.misc import LoadBalanceBehavior, ReadBalanceBehavior

if TYPE_CHECKING:
    from ravendb.http.request_executor import RequestExecutor
    from ravendb.documents.queries.misc import Query
    from ravendb.documents.session.operations.query import QueryOperation
    from ravendb.documents.session.document_session_operations.in_memory_document_session_operations import (
        InMemoryDocumentSessionOperations,
    )
    from ravendb.documents.store.definition import DocumentStore
    from ravendb.http.server_node import ServerNode

_T_Key = TypeVar("_T_Key")
_T_Value = TypeVar("_T_Value")


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
    _client_session_id_counter = threading.local()
    _client_session_id_counter.counter = 0

    def __init__(
        self, session: InMemoryDocumentSessionOperations, options: SessionOptions, document_store: DocumentStore
    ):
        if not document_store:
            raise ValueError("DocumentStore cannot be None")
        if not session:
            raise ValueError("Session cannot be None")
        self._session = session
        self._session_id: Union[None, int] = None
        self._session_id_used: Union[None, bool] = None
        self._load_balancer_context_seed = session.request_executor.conventions.load_balancer_context_seed
        self._can_use_load_balance_behavior = (
            session.conventions.load_balance_behavior == LoadBalanceBehavior.USE_SESSION_CONTEXT
            and session.conventions.load_balancer_per_session_context_selector is not None
        )
        self.document_store = document_store
        self.no_caching = options.no_caching

        self.last_cluster_transaction_index: Union[None, int] = None

    @property
    def can_use_load_balance_behavior(self) -> bool:
        return self._can_use_load_balance_behavior

    @property
    def database_name(self) -> str:
        return self._session.database_name

    @property
    def session_id(self) -> int:
        if self._session_id is None:
            context = None
            selector = self._session.conventions.load_balancer_per_session_context_selector
            if selector is not None:
                context = selector(self._session.database_name)
            self._set_context_internal(context)
        self._session_id_used = True
        return self._session_id

    @property
    def context(self):
        # placeholder for convenient setter
        return None

    @context.setter
    def context(self, session_id: str):
        if not session_id or session_id.isspace():
            raise ValueError("Session key cannot be None or whitespace")

        self._set_context_internal(session_id)

        self._can_use_load_balance_behavior = (
            self._can_use_load_balance_behavior
            or self._session.conventions.load_balance_behavior == LoadBalanceBehavior.USE_SESSION_CONTEXT
        )

    def _set_context_internal(self, session_id: str) -> None:
        if self._session_id_used:
            raise RuntimeError(
                "Unable to set the session context after it has already been used. "
                "The session context can only be modified before it is utilized."
            )

        if session_id is None:
            v = self._client_session_id_counter.counter
            self._session_id = v
            v += 1
            self._client_session_id_counter = v
        else:
            self._session_id = int(hashlib.md5(session_id.encode("utf-8")).hexdigest(), 16)

    def increment_request_count(self) -> None:
        self._session.increment_requests_count()

    def get_current_session_node(self, request_executor: RequestExecutor) -> ServerNode:
        if request_executor.conventions.load_balance_behavior == LoadBalanceBehavior.USE_SESSION_CONTEXT:
            if self._can_use_load_balance_behavior:
                result = request_executor.get_node_by_session_id(self.session_id)
                return result.current_node

        read_balance_behavior = request_executor.conventions.read_balance_behavior

        if read_balance_behavior == ReadBalanceBehavior.NONE:
            result = request_executor.preferred_node
        elif read_balance_behavior == ReadBalanceBehavior.ROUND_ROBIN:
            result = request_executor.get_node_by_session_id(self.session_id)
        elif read_balance_behavior == ReadBalanceBehavior.FASTEST_NODE:
            result = request_executor.get_fastest_node()
        else:
            raise ValueError(f"Unsupported read balance behavior '{str(read_balance_behavior)}'")

        return result.current_node


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

    def with_tag(self, tag: str) -> "DocumentQueryCustomization":
        self.query._with_tag(tag)
        return self


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
        return f"val_{self.__arg_counter - 1}_{self.__suffix}"

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

    def remove_all(self, predicate_js: str) -> "JavaScriptArray":
        path = self.__path_to_array
        self.__script_lines.append(f"this.{path} = this.{path}.filter(function(item){{ return !({predicate_js}); }});")
        return self


class JavaScriptMap(Generic[_T_Key, _T_Value]):
    def __init__(self, suffix: int, path_to_map: str):
        self._suffix = suffix
        self._path_to_map = path_to_map
        self._arg_counter = 0
        self._script_lines = []
        self._parameters: Dict[str, object] = {}

    @property
    def script(self) -> str:
        return "\r".join(self._script_lines)

    @property
    def parameters(self) -> Dict[str, object]:
        return self._parameters

    def _get_next_argument_name(self) -> str:
        self._arg_counter += 1
        return f"val_{self._arg_counter - 1}_{self._suffix}"

    def put(self, key: _T_Key, value: _T_Value) -> JavaScriptMap[_T_Key, _T_Value]:
        argument_name = self._get_next_argument_name()

        self._script_lines.append(f"this.{self._path_to_map}.{key} = args.{argument_name};")
        self.parameters[argument_name] = value
        return self

    def remove(self, key: _T_Key) -> JavaScriptMap[_T_Key, _T_Value]:
        self._script_lines.append(f"delete this.{self._path_to_map}.{key};")


class MethodCall(ABC):
    def __init__(self, args: List[object] = None, access_path: str = None):
        self.args = args
        self.access_path = access_path


class CmpXchg(MethodCall):
    @classmethod
    def value(cls, key: str) -> CmpXchg:
        # Kept for back-compat; prefer RavenDocumentQuery.cmp_xchg().
        import warnings

        warnings.warn(
            "CmpXchg.value is deprecated; use RavenDocumentQuery.cmp_xchg() instead.",
            DeprecationWarning,
            stacklevel=2,
        )
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

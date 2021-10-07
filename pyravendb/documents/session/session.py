import datetime
from enum import Enum

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

        self.options = options
        self.document_store = document_store


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

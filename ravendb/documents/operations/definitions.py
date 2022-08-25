from abc import abstractmethod
from typing import TypeVar, TYPE_CHECKING, Generic

from ravendb.http.http_cache import HttpCache

_T = TypeVar("_T")
_Operation_T = TypeVar("_Operation_T")

if TYPE_CHECKING:
    from ravendb.documents.conventions import DocumentConventions
    from ravendb.documents.store.definition import DocumentStore
    from ravendb.http.raven_command import RavenCommand, VoidRavenCommand


class IOperation(Generic[_Operation_T]):
    def get_command(
        self, store: "DocumentStore", conventions: "DocumentConventions", cache: HttpCache
    ) -> "RavenCommand[_Operation_T]":
        pass


class VoidOperation(IOperation[None]):
    @abstractmethod
    def get_command(
        self, store: "DocumentStore", conventions: "DocumentConventions", cache: HttpCache
    ) -> "VoidRavenCommand":
        pass


class MaintenanceOperation(Generic[_T]):
    @abstractmethod
    def get_command(self, conventions: "DocumentConventions") -> "RavenCommand[_T]":
        pass


class VoidMaintenanceOperation(MaintenanceOperation[None]):
    @abstractmethod
    def get_command(self, conventions: "DocumentConventions") -> "VoidRavenCommand":
        pass


class OperationIdResult:
    def __init__(self, operation_id: int = None, operation_node_tag: str = None):
        self.operation_id = operation_id
        self.operation_node_tag = operation_node_tag


class OperationExceptionResult:
    def __init__(self, type: str, message: str, error: str, status_code: int):
        self.type = type
        self.message = message
        self.error = error
        self.status_code = status_code

from abc import abstractmethod, ABC
from typing import TYPE_CHECKING, Generic, TypeVar, Optional

if TYPE_CHECKING:
    from ravendb.documents.commands.multi_get import GetRequest, GetResponse
    from ravendb.documents.queries.query import QueryResult

_T = TypeVar("_T")


class LazyOperation(Generic[_T], ABC):
    @abstractmethod
    def __init__(self):
        self.query_result: Optional["QueryResult"] = None
        self.result: _T = None

    @property
    @abstractmethod
    def is_requires_retry(self) -> bool:
        pass

    @abstractmethod
    def handle_response(self, response: "GetResponse") -> None:
        pass

    @abstractmethod
    def create_request(self) -> Optional["GetRequest"]:
        pass

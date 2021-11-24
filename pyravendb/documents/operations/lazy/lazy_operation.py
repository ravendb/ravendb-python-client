from abc import abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyravendb.documents.commands.multi_get import GetRequest, GetResponse
    from pyravendb.documents.queries.query import QueryResult


class LazyOperation:
    @abstractmethod
    def query_result(self) -> "QueryResult":
        pass

    @abstractmethod
    def result(self) -> object:
        pass

    @abstractmethod
    def is_requires_retry(self) -> bool:
        pass

    @abstractmethod
    def handle_response(self, response: "GetResponse") -> None:
        pass

    @abstractmethod
    def create_request(self) -> "GetRequest":
        pass

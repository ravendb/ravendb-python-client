from abc import abstractmethod
from typing import Callable, Union

import requests

from pyravendb.documents.commands.multi_get import GetRequest, GetResponse
from pyravendb.documents.queries.query import QueryResult


class LazyOperation:
    @abstractmethod
    @property
    def query_result(self) -> QueryResult:
        pass

    @abstractmethod
    @property
    def result(self) -> object:
        pass

    @abstractmethod
    @property
    def is_requires_retry(self) -> bool:
        pass

    @abstractmethod
    def handle_response(self, response: GetResponse) -> None:
        pass

    @abstractmethod
    def create_request(self) -> GetRequest:
        pass

from __future__ import annotations

import datetime
import json
from typing import Union, List, Generic, TypeVar, Type, Callable, Dict, TYPE_CHECKING

from pyravendb.documents.queries.facets.misc import FacetResult
from pyravendb.documents.queries.index_query import IndexQuery
from pyravendb.tools.utils import Utils
from pyravendb.documents.session.entity_to_json import EntityToJson
from pyravendb.documents.queries.query import QueryResult
from pyravendb.extensions.json_extensions import JsonExtensions
from pyravendb.documents.commands.crud import GetDocumentsResult
from pyravendb.documents.session.operations.load_operation import LoadOperation
from pyravendb.documents.conventions.document_conventions import DocumentConventions
from pyravendb.documents.operations.lazy.lazy_operation import LazyOperation
from pyravendb.documents.commands.multi_get import Content, GetRequest, GetResponse

if TYPE_CHECKING:
    from pyravendb.documents.session.operations.query import QueryOperation
    from pyravendb.documents.session.in_memory_document_session_operations import InMemoryDocumentSessionOperations

_T = TypeVar("_T")


class IndexQueryContent(Content):
    def __init__(self, conventions: DocumentConventions, query: IndexQuery):
        self.__conventions = conventions
        self.__query = query

    def write_content(self) -> dict:
        return JsonExtensions.write_index_query(self.__conventions, self.__query)


# ------- OPERATIONS --------
class LazyLoadOperation(LazyOperation):
    def __init__(
        self, object_type: Type[_T], session: "InMemoryDocumentSessionOperations", load_operation: LoadOperation
    ):
        self.__object_type = object_type
        self.__session = session
        self.__load_operation = load_operation
        self.__keys: Union[None, List[str]] = None
        self.__includes: Union[None, List[str]] = None
        self.__already_in_session: Union[None, List[str]] = None

        self.__result: Union[None, List[_T]] = None
        self.__query_result: Union[None, QueryResult] = None
        self.__requires_retry: Union[None, bool] = None

    @property
    def query_result(self) -> QueryResult:
        return self.__query_result

    @query_result.setter
    def query_result(self, value) -> None:
        self.__query_result = value

    @property
    def result(self) -> List[_T]:
        return self.__result

    @result.setter
    def result(self, value) -> None:
        self.__result = value

    @property
    def is_requires_retry(self) -> bool:
        return self.__requires_retry

    @is_requires_retry.setter
    def is_requires_retry(self, value) -> None:
        self.__requires_retry = value

    def create_request(self) -> GetRequest:
        query_builder = ["?"]
        if self.__includes is not None:
            for include in self.__includes:
                query_builder.append(f"&include={include}")

        has_items = False

        for key in self.__keys:
            if self.__session.is_loaded_or_deleted(key):
                self.__already_in_session.append(key)
            else:
                has_items = True
                query_builder.append(f"&id={Utils.escape(key,None,None)}")

        if not has_items:
            self.result = self.__load_operation.get_documents(self.__object_type)
            return None

        get_request = GetRequest()
        get_request.url = "/docs"
        get_request.query = "".join(query_builder)
        return get_request

    def by_keys(self, *keys: str) -> LazyLoadOperation:
        self.__keys = list(set(filter(lambda key: key, keys)))
        return self

    def with_includes(self, *includes: str) -> LazyLoadOperation:
        self.__includes = includes
        return self

    def handle_response(self, response: GetResponse) -> None:
        if response.force_retry:
            self.result = None
            self.__requires_retry = True
            return

        # todo: make sure it's working
        multi_load_result = (
            EntityToJson.convert_to_entity_static(
                json.loads(response.result), GetDocumentsResult, self.__session.conventions, None, None
            )
            if response.result
            else None
        )

    def __handle_response(self, load_result: GetDocumentsResult) -> None:
        if self.__already_in_session:
            # push this to the session
            LoadOperation(self.__session).by_keys(self.__already_in_session).get_documents(self.__object_type)

        self.__load_operation.set_result(load_result)
        if not self.__requires_retry:
            self.result = self.__load_operation.get_documents(self.__object_type)


class LazyQueryOperation(Generic[_T], LazyOperation[_T]):
    def __init__(
        self,
        object_type: Type[_T],
        session: "InMemoryDocumentSessionOperations",
        query_operation: QueryOperation,
        after_query_executed: List[Callable[[QueryResult], None]],
    ):
        self.__object_type = object_type
        self.__session = session
        self.__query_operation = query_operation
        self.__after_query_executed = after_query_executed

        self.__result: Union[None, List[_T]] = None
        self.__query_result: Union[None, QueryResult] = None
        self.__requires_retry: Union[None, bool] = None

    def create_request(self) -> "GetRequest":
        request = GetRequest()
        request.can_cache_aggressively = (
            not self.__query_operation.index_query.disable_caching
            and not self.__query_operation.index_query.wait_for_non_stale_results
        )
        request.url = "/queries"
        request.method = "POST"
        request.query = f"?queryHash={self.__query_operation.index_query.get_query_hash()}"
        request.content = IndexQueryContent(self.__session.conventions, self.__query_operation.index_query)
        return request

    @property
    def result(self) -> Union[None, List[_T]]:
        return

    @result.setter
    def result(self, value: List[_T]):
        self.__result = value

    @property
    def query_result(self) -> QueryResult:
        return self.__query_result

    @query_result.setter
    def query_result(self, value: QueryResult):
        self.__query_result = value

    @property
    def is_requires_retry(self) -> bool:
        return self.__requires_retry

    @is_requires_retry.setter
    def is_requires_retry(self, value: bool):
        self.__requires_retry = value

    def handle_response(self, response: "GetResponse") -> None:
        if response.force_retry:
            self.result = None
            self.__requires_retry = True
            return

        query_result = None

        if response.result is not None:
            query_result = QueryResult.from_json(json.loads(response.result))

        self.__handle_response(query_result, response.elapsed)

    def __handle_response(self, query_result: QueryResult, duration: datetime.timedelta):
        self.__query_operation.ensure_is_acceptable_and_save_result(query_result, duration)

        for event in self.__after_query_executed:
            event(query_result)

        self.__result = self.__query_operation.complete(self.__object_type)
        self.__query_result = query_result


class LazyAggregationQueryOperation(LazyOperation):
    def __init__(
        self,
        session: "InMemoryDocumentSessionOperations",
        index_query: IndexQuery,
        invoke_after_query_executed: Callable[[QueryResult], None],
        process_results: Callable[[QueryResult], Dict[str, FacetResult]],
    ):
        self.__session = session
        self.__index_query = index_query
        self.__invoke_after_query_executed = invoke_after_query_executed
        self.__process_results = process_results

        self.result: Union[None, object] = None
        self.query_result: Union[None, QueryResult] = None
        self.requires_retry: Union[None, bool] = None

    def create_request(self) -> "GetRequest":
        request = GetRequest()
        request.url = "/queries"
        request.method = "POST"
        request.query = f"?queryHash={self.__index_query.get_query_hash()}"
        request.content = IndexQueryContent(self.__session.conventions, self.__index_query)
        return request

    def handle_response(self, response: "GetResponse") -> None:
        if response.force_retry:
            self.result = None
            self.requires_retry = True
            return

        query_result = QueryResult.from_json(json.loads(response.result))
        self.__handle_response(query_result)

    def __handle_response(self, query_result: QueryResult) -> None:
        self.result = self.__process_results(query_result)
        self.query_result = query_result


class LazySuggestionQueryOperation(LazyOperation):
    def __init__(
        self,
        session: "InMemoryDocumentSessionOperations",
        index_query: IndexQuery,
        invoke_after_query_executed: Callable[[QueryResult], None],
        process_results: Callable[[QueryResult], Dict[str, FacetResult]],
    ):
        self.__session = session
        self.__index_query = index_query
        self.__invoke_after_query_executed = invoke_after_query_executed
        self.__process_results = process_results

        self.result: Union[None, object] = None
        self.query_result: Union[None, QueryResult] = None
        self.requires_retry: Union[None, bool] = None

    def create_request(self) -> "GetRequest":
        request = GetRequest()
        request.can_cache_aggressively = (
            not self.__index_query.disable_caching and self.__index_query.wait_for_non_stale_results
        )
        request.url = "/queries"
        request.method = "POST"
        request.query = f"?queryHash={self.__index_query.get_query_hash()}"
        request.content = IndexQueryContent(self.__session.conventions, self.__index_query)
        return request

    def handle_response(self, response: "GetResponse") -> None:
        if response.force_retry:
            self.result = None
            self.requires_retry = True
            return

        query_result = QueryResult.from_json(json.loads(response.result))
        self.__handle_response(query_result)

    def __handle_response(self, query_result: QueryResult) -> None:
        self.result = self.__process_results(query_result)
        self.query_result = query_result

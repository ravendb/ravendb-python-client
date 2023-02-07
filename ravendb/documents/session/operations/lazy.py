from __future__ import annotations

import datetime
import json
from http import HTTPStatus
from typing import Union, List, Generic, TypeVar, Type, Callable, Dict, TYPE_CHECKING, Optional

from ravendb.documents.operations.compare_exchange.compare_exchange_value_result_parser import (
    CompareExchangeValueResultParser,
)

from ravendb import constants
from ravendb.documents.queries.facets.misc import FacetResult
from ravendb.documents.queries.index_query import IndexQuery
from ravendb.documents.session.document_info import DocumentInfo
from ravendb.documents.session.loaders.loaders import LazyMultiLoaderWithInclude
from ravendb.documents.store.lazy import Lazy
from ravendb.documents.session.conditional_load import ConditionalLoadResult
from ravendb.exceptions.raven_exceptions import RavenException
from ravendb.tools.utils import Utils, CaseInsensitiveDict
from ravendb.documents.queries.query import QueryResult
from ravendb.extensions.json_extensions import JsonExtensions
from ravendb.documents.commands.crud import GetDocumentsResult, ConditionalGetDocumentsCommand
from ravendb.documents.session.operations.load_operation import LoadOperation
from ravendb.documents.conventions import DocumentConventions
from ravendb.documents.operations.lazy.lazy_operation import LazyOperation
from ravendb.documents.commands.multi_get import Content, GetRequest, GetResponse

if TYPE_CHECKING:
    from ravendb.documents.session.operations.query import QueryOperation
    from ravendb.documents.session.in_memory_document_session_operations import InMemoryDocumentSessionOperations
    from ravendb.documents.session.document_session import DocumentSession
    from ravendb.documents.session.cluster_transaction_operation import (
        ClusterTransactionOperationsBase,
    )

_T = TypeVar("_T")


class IndexQueryContent(Content):
    def __init__(self, conventions: DocumentConventions, query: IndexQuery):
        self.__conventions = conventions
        self.__query = query

    def write_content(self) -> dict:
        return JsonExtensions.write_index_query(self.__conventions, self.__query)


class LazyStartsWithOperation(LazyOperation):
    def __init__(
        self,
        object_type: Type[_T],
        prefix: str,
        matches: str,
        exclude: str,
        start: int,
        page_size: int,
        session_operations: "InMemoryDocumentSessionOperations",
        start_after: str,
    ):
        self.__object_type = object_type
        self.__prefix = prefix
        self.__matches = matches
        self.__exclude = exclude
        self.__start = start
        self.__page_size = page_size
        self.__session_operations = session_operations
        self.__start_after = start_after

        self.result = None
        self.query_result: Union[None, QueryResult] = None
        self.requires_retry: Union[None, bool] = None

    @property
    def is_requires_retry(self) -> bool:
        return self.requires_retry

    def create_request(self) -> GetRequest:
        request = GetRequest()
        request.url = "/docs"
        request.query = (
            f"?startsWith={Utils.quote_key(self.__prefix)}"
            f"&matches={Utils.quote_key(self.__matches or '')}"
            f"&exclude={Utils.quote_key(self.__exclude or '')}"
            f"&start={self.__start}&pageSize={self.__page_size}"
            f"&startAfter={self.__start_after or ''}"
        )
        return request

    def handle_response(self, response: GetResponse) -> None:
        get_documents_result = GetDocumentsResult.from_json(json.loads(response.result))
        final_results = CaseInsensitiveDict()

        for document in get_documents_result.results:
            new_document_info = DocumentInfo.get_new_document_info(document)
            self.__session_operations._documents_by_id.add(new_document_info)

            if new_document_info.key is None:
                continue  # todo: is this possible?

            if self.__session_operations.is_deleted(new_document_info.key):
                final_results[new_document_info.key] = None
                continue

            doc = self.__session_operations._documents_by_id.get_value(new_document_info.key)
            if doc is not None:
                final_results[new_document_info.key] = self.__session_operations.track_entity_document_info(
                    self.__object_type, doc
                )
                continue

            final_results[new_document_info.key] = None

        self.result = final_results


class LazyConditionalLoadOperation(LazyOperation):
    def __init__(
        self, object_type: Type[_T], key: str, change_vector: str, session: "InMemoryDocumentSessionOperations"
    ):
        self.__object_type = object_type
        self.__key = key
        self.__change_vector = change_vector
        self.__session = session
        self.__result: Union[None, object] = None
        self.__requires_retry: Union[None, bool] = None

    @property
    def result(self) -> object:
        return self.__result

    @property
    def query_result(self) -> QueryResult:
        raise NotImplementedError()

    @property
    def is_requires_retry(self) -> bool:
        return self.__requires_retry

    def create_request(self) -> GetRequest:
        request = GetRequest()

        request.url = "/docs"
        request.method = "GET"
        request.query = f"?id={Utils.quote_key(self.__key)}"

        request.headers["If-None-Match"] = f'"{self.__change_vector}"'
        return request

    def handle_response(self, response: "GetResponse") -> None:
        if response.force_retry:
            self.__result = None
            self.__requires_retry = None
            return

        if response.status_code == HTTPStatus.NOT_MODIFIED:
            self.__result = ConditionalLoadResult.create(None, self.__change_vector)
            return

        elif response.status_code == HTTPStatus.NOT_FOUND:
            self.__session.register_missing(self.__key)
            self.__result = ConditionalLoadResult.create(None, None)
            return

        try:
            if response.result is not None:
                etag = response.headers.get(constants.Headers.ETAG)

                res = ConditionalGetDocumentsCommand.ConditionalGetResult.from_json(json.loads(response.result))
                document_info = DocumentInfo.get_new_document_info(res.results[0])
                r = self.__session.track_entity_document_info(self.__object_type, document_info)

                self.__result = ConditionalLoadResult.create(r, etag)
                return

            self.__result = None
            self.__session.register_missing(self.__key)
        except Exception as e:
            raise RuntimeError(e)


class LazySessionOperations:
    def __init__(self, delegate: DocumentSession):
        self._delegate = delegate

    def include(self, path: str) -> LazyMultiLoaderWithInclude:
        return LazyMultiLoaderWithInclude(self._delegate).include(path)

    def load(
        self, object_type: Type[_T], ids: Union[List[str], str], on_eval: Callable = None
    ) -> Optional[Lazy[Union[Dict[str, object], object]]]:
        if not ids:
            return None

        if isinstance(ids, str):
            key = ids
            if self._delegate.advanced.is_loaded(key):
                return Lazy(lambda: self._delegate.load(key, object_type))

            lazy_load_operation = LazyLoadOperation(
                object_type, self._delegate, LoadOperation(self._delegate).by_key(key)
            ).by_key(key)

            return self._delegate.add_lazy_operation(object_type, lazy_load_operation, on_eval)

        elif isinstance(ids, list):
            return self._delegate.lazy_load_internal(object_type, ids, [], on_eval)

        raise TypeError("Expected 'ids' as 'str' or 'list[str]'")

    def load_starting_with(
        self,
        object_type: Type[_T],
        id_prefix: str,
        matches: str = None,
        start: int = 0,
        page_size: int = 25,
        exclude: str = None,
        start_after: str = None,
    ) -> Lazy[Dict[str, _T]]:
        operation = LazyStartsWithOperation(
            object_type, id_prefix, matches, exclude, start, page_size, self._delegate, start_after
        )

        return self._delegate.add_lazy_operation(dict, operation, None)

    def conditional_load(
        self, key: str, change_vector: str, object_type: Type[_T] = None
    ) -> Lazy[ConditionalLoadResult[_T]]:
        if not key.isspace():
            raise ValueError("key cannot be None or whitespace")

        if self._delegate.is_loaded(key):

            def __lazy_factory():
                entity = self._delegate.load(key, object_type)
                if entity is None:
                    return ConditionalLoadResult.create(None, None)
                cv = self._delegate.get_change_vector_for(entity)
                return ConditionalLoadResult.create(entity, cv)

            return Lazy(__lazy_factory)

        if change_vector.isspace():
            raise ValueError(
                f"The requested document with id '{key}' is not loaded into the session and could not "
                f"conditional load when change_vector is None or empty"
            )

        lazy_load_operation = LazyConditionalLoadOperation(object_type, key, change_vector, self._delegate)
        return self._delegate.add_lazy_operation(ConditionalLoadResult, lazy_load_operation, None)


class LazyLoadOperation(LazyOperation):
    def __init__(
        self, object_type: Type[_T], session: "InMemoryDocumentSessionOperations", load_operation: LoadOperation
    ):
        self.__object_type = object_type
        self.__session = session
        self.__load_operation = load_operation
        self.__keys: Union[None, List[str]] = None
        self.__includes: Union[None, List[str]] = None
        self.__already_in_session: List[str] = []

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

    def create_request(self) -> Optional[GetRequest]:
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

    def by_key(self, key: str) -> LazyLoadOperation:
        if not key or key.isspace():
            return self

        if self.__keys is None:
            self.__keys = [key]

        return self

    def by_keys(self, keys: List[str]) -> LazyLoadOperation:
        self.__keys = list(set(filter(lambda key: key, keys)))
        return self

    def with_includes(self, *includes: str) -> LazyLoadOperation:
        self.__includes = includes
        return self

    def handle_response(self, response: GetResponse) -> None:
        if response.force_retry:
            self.result = None
            self.requires_retry = True
            return
        json_result = json.loads(response.result)
        multi_load_result = None if json_result is None else GetDocumentsResult.from_json(json_result)
        self.__handle_response(multi_load_result)

    def __handle_response(self, load_result: GetDocumentsResult) -> None:
        if self.__already_in_session:
            # push this to the session
            LoadOperation(self.__session).by_keys(self.__already_in_session).get_documents(self.__object_type)

        self.__load_operation.set_result(load_result)
        if not self.is_requires_retry:
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
        return self.__result

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

    def handle_response(self, response: "GetResponse") -> None:
        if response.force_retry:
            self.result = None
            self.__requires_retry = True
            return

        query_result = None

        if response.result is not None:
            json_response = json.loads(response.result)
            query_result = None if json_response is None else QueryResult.from_json(json_response)

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
        self.__requires_retry: Union[None, bool] = None

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
            self.__requires_retry = True
            return

        query_result = QueryResult.from_json(json.loads(response.result))
        self.__handle_response(query_result)

    def __handle_response(self, query_result: QueryResult) -> None:
        self.result = self.__process_results(query_result)
        self.query_result = query_result

    @property
    def is_requires_retry(self) -> bool:
        return self.__requires_retry


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
        self.__requires_retry: Union[None, bool] = None

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
            self.__requires_retry = True
            return

        query_result = QueryResult.from_json(json.loads(response.result))
        self.__handle_response(query_result)

    def __handle_response(self, query_result: QueryResult) -> None:
        self.result = self.__process_results(query_result)
        self.query_result = query_result

    @property
    def is_requires_retry(self) -> bool:
        return self.__requires_retry


class LazyGetCompareExchangeValueOperation(Generic[_T], LazyOperation[_T]):
    def __init__(
        self,
        cluster_session: ClusterTransactionOperationsBase,
        key: str,
        conventions: DocumentConventions,
        object_type: Type[_T] = None,
    ):
        if cluster_session is None:
            raise ValueError("Cluster session cannot be None")
        if conventions is None:
            raise ValueError("Conventions cannot be None")
        if key is None:
            raise ValueError("Key cannot be None")

        self.__cluster_session = cluster_session
        self.__object_type = object_type
        self.__conventions = conventions
        self.__key = key

        self.__result = None
        self.__requires_retry = None

    @property
    def result(self) -> object:
        return self.__result

    @property
    def query_result(self) -> QueryResult:
        raise NotImplementedError("Not implemented")

    @property
    def is_requires_retry(self) -> bool:
        return self.__requires_retry

    def create_request(self) -> Optional["GetRequest"]:
        if self.__cluster_session.is_tracked(self.__key):
            self.__result, not_tracked = self.__cluster_session.get_compare_exchange_value_from_session_internal(
                self.__key, self.__object_type
            )
            return None

        request = GetRequest()
        request.url = "/cmpxchg"
        request.method = "GET"
        query_builder = f"?key={Utils.quote_key(self.__key)}"
        request.query = query_builder

        return request

    def handle_response(self, response: "GetResponse") -> None:
        if response.force_retry:
            self.__result = None
            self.__requires_retry = True
            return

        try:
            if response.result is not None:
                value = CompareExchangeValueResultParser.get_value(dict, response.result, False, self.__conventions)

                if self.__cluster_session.session.no_tracking:
                    if value is None:
                        self.__result = self.__cluster_session.register_missing_compare_exchange_value(
                            self.__key
                        ).get_value(self.__object_type, self.__conventions)
                        return

                    self.__result = self.__cluster_session.register_compare_exchange_value(value).get_value(
                        self.__object_type, self.__conventions
                    )
                    return

                if value is not None:
                    self.__cluster_session.register_compare_exchange_value(value)

            if not self.__cluster_session.is_tracked(self.__key):
                self.__cluster_session.register_missing_compare_exchange_value(self.__key)

            self.__result, not_tracked = self.__cluster_session.get_compare_exchange_value_from_session_internal(
                self.__key, self.__object_type
            )
        except Exception as e:
            raise RavenException(f"Unable to get compare exchange value: {self.__key}", e)


class LazyGetCompareExchangeValuesOperation(LazyOperation[_T], Generic[_T]):
    def __init__(
        self,
        cluster_session: ClusterTransactionOperationsBase,
        keys: List[str],
        conventions: DocumentConventions,
        object_type: Type[_T] = None,
        starts_with: str = None,
        start: int = 0,
        page_size: int = 0,
    ):
        if not keys:
            raise ValueError("Keys cannot be None or empty")

        if cluster_session is None:
            raise ValueError("Cluster session cannot be None")

        if conventions is None:
            raise ValueError("Conventions cannot be None")

        self.__cluster_session = cluster_session
        self.__keys = keys
        self.__conventions = conventions
        self.__object_type = object_type

        self.__start = start
        self.__page_size = page_size
        self.__starts_with = starts_with

        self.__result: Optional[object] = None
        self.__requires_retry: Optional[bool] = None

    @property
    def result(self) -> object:
        return self.__result

    @property
    def query_result(self) -> QueryResult:
        raise NotImplementedError("Not implemented.")

    @property
    def is_requires_retry(self) -> bool:
        return self.__requires_retry

    def create_request(self) -> Optional["GetRequest"]:
        path_builder = None

        if self.__keys:
            for key in self.__keys:
                if self.__cluster_session.is_tracked(key):
                    continue

                if path_builder is None:
                    path_builder = ["?"]

                path_builder.append("&key=")
                path_builder.append(Utils.quote_key(key))

        else:
            path_builder = ["?"]
            if self.__starts_with and not self.__starts_with.isspace():
                path_builder.append("&startsWith")
                path_builder.append(Utils.quote_key(self.__starts_with))
            path_builder.append("&start=")
            path_builder.append(self.__start)
            path_builder.append("&pageSize=")
            path_builder.append(self.__page_size)

        if path_builder is None:
            self.__result, _ = self.__cluster_session.get_compare_exchange_values_from_session_internal(
                self.__keys, self.__object_type
            )
            return None

        request = GetRequest()
        request.url = "/cmpxchg"
        request.method = "GET"
        request.query = "".join(path_builder)

        return request

    def handle_response(self, response: "GetResponse") -> None:
        if response.force_retry:
            self.__result = None
            self.__requires_retry = True
            return

        try:
            if response.result is not None:
                if self.__cluster_session.session.no_tracking:
                    result = CaseInsensitiveDict()
                    for key, value in CompareExchangeValueResultParser.get_values(
                        dict, response.result, False, self.__conventions
                    ).items():
                        if value is None:
                            result[key] = self.__cluster_session.register_missing_compare_exchange_value(key).get_value(
                                self.__object_type, self.__conventions
                            )
                            continue

                        result[key] = self.__cluster_session.register_compare_exchange_value(value).get_value(
                            self.__object_type, self.__conventions
                        )
                    self.__result = result
                    return

                for key, value in CompareExchangeValueResultParser.get_values(
                    dict, response.result, False, self.__conventions
                ).items():
                    if value is None:
                        continue

                    self.__cluster_session.register_compare_exchange_value(value)

            if self.__keys is not None:
                for key in self.__keys:
                    if self.__cluster_session.is_tracked(key):
                        continue

                    self.__cluster_session.register_missing_compare_exchange_value(key)

            self.__result, _ = self.__cluster_session.get_compare_exchange_values_from_session_internal(
                self.__keys, self.__object_type
            )

        except Exception:
            raise RavenException(f"Unable to get compare exchange values:  {' ,'.join(self.__keys)}")

import json
from http import HTTPStatus
from typing import Callable, Dict, Union, TYPE_CHECKING, List
from pyravendb import constants
from pyravendb.commands.commands_results import GetDocumentsResult
from pyravendb.documents.commands import ConditionalGetDocumentsCommand
from pyravendb.documents.commands.multi_get import GetRequest, GetResponse
from pyravendb.documents.lazy import ConditionalLoadResult, Lazy
from pyravendb.documents.operations.lazy.lazy_operation import LazyOperation
from pyravendb.documents.queries.query import QueryResult
from pyravendb.documents.session.document_info import DocumentInfo
from pyravendb.documents.session.loaders import LazyMultiLoaderWithInclude
from pyravendb.documents.session.operations.lazy.lazy import LazyLoadOperation
from pyravendb.raven_operations.load_operation import LoadOperation
from pyravendb.tools.utils import Utils, CaseInsensitiveDict

if TYPE_CHECKING:
    from pyravendb.documents import InMemoryDocumentSessionOperations, DocumentSession


class LazyStartsWithOperation(LazyOperation):
    def __init__(
        self,
        object_type: type,
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
        try:
            get_documents_result = GetDocumentsResult.from_json(json.loads(response.result))
            final_results = CaseInsensitiveDict()

            for document in get_documents_result.results:
                new_document_info = DocumentInfo.get_new_document_info(document)
                self.__session_operations.documents_by_id.add(new_document_info)

                if new_document_info.key is None:
                    continue  # is this possible?

                if self.__session_operations.is_deleted(new_document_info.key):
                    final_results[new_document_info.key] = None
                    continue

                doc = self.__session_operations.documents_by_id.get_value(new_document_info.key)
                if doc is not None:
                    final_results[new_document_info.key] = self.__session_operations.track_entity(
                        self.__object_type, doc
                    )
                    continue

                final_results[new_document_info.key] = None

            self.result = final_results

        except Exception as e:
            raise RuntimeError(e)


class LazyConditionalLoadOperation(LazyOperation):
    def __init__(self, object_type: type, key: str, change_vector: str, session: "InMemoryDocumentSessionOperations"):
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

        if response.status_code == HTTPStatus.NOT_MODIFIED.value:
            self.__result = ConditionalLoadResult.create(None, self.__change_vector)
            return

        elif response.status_code == HTTPStatus.NOT_FOUND.value:
            self.__session.register_missing(self.__key)
            self.__result = ConditionalLoadResult.create(None, None)
            return

        try:
            if response.result is not None:
                etag = response.headers.get(constants.Headers.ETAG)

                res = ConditionalGetDocumentsCommand.ConditionalGetResult.from_json(json.loads(response.result))
                document_info = DocumentInfo.get_new_document_info(res.results[0])
                r = self.__session.track_entity(self.__object_type, document_info)

                self.__result = ConditionalLoadResult.create(r, etag)
                return

            self.__result = None
            self.__session.register_missing(self.__key)
        except Exception as e:
            raise RuntimeError(e)


class LazySessionOperations:
    def __init__(self, delegate: "DocumentSession"):
        self._delegate = delegate

    def include(self, path: str) -> LazyMultiLoaderWithInclude:
        return LazyMultiLoaderWithInclude(self._delegate).include(path)

    def load(
        self, object_type: type, ids: Union[List[str], str], on_eval: Callable = None
    ) -> Lazy[Union[Dict[str, object], object]]:
        if not ids:
            return None

        if isinstance(ids, str):
            key = ids
            if self._delegate.is_loaded(key):
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
        object_type: type,
        id_prefix: str,
        matches: str = None,
        start: int = 0,
        page_size: int = 25,
        exclude: str = None,
        start_after: str = None,
    ) -> Lazy[Dict[str, object]]:
        operation = LazyStartsWithOperation(
            object_type, id_prefix, matches, exclude, start, page_size, self._delegate, start_after
        )

        return self._delegate.add_lazy_operation(dict, operation, None)

    def conditional_load(self, object_type: type, key: str, change_vector: str):
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

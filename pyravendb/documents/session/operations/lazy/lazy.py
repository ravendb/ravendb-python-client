from __future__ import annotations

import json
from typing import Union
from pyravendb.tools.utils import Utils
from pyravendb.data.query import IndexQuery
from pyravendb.store.entity_to_json import EntityToJson
from pyravendb.documents.queries.query import QueryResult
from pyravendb.extensions.json_extensions import JsonExtensions
from pyravendb.commands.commands_results import GetDocumentsResult
from pyravendb.raven_operations.load_operation import LoadOperation
from pyravendb.data.document_conventions import DocumentConventions
from pyravendb.documents.operations.lazy.lazy_operation import LazyOperation
from pyravendb.documents.commands.multi_get import Content, GetRequest, GetResponse
from pyravendb.documents.session.in_memory_document_session_operations import InMemoryDocumentSessionOperations


class IndexQueryContent(Content):
    def __init__(self, conventions: DocumentConventions, query: IndexQuery):
        self.__conventions = conventions
        self.__query = query

    def write_content(self) -> dict:
        return JsonExtensions.write_index_query(self.__conventions, self.__query)


# ------- OPERATIONS --------
class LazyLoadOperation(LazyOperation):
    def __init__(self, object_type: type, session: InMemoryDocumentSessionOperations, load_operation: LoadOperation):
        self.__object_type = object_type
        self.__session = session
        self.__load_operation = load_operation
        self.__keys: Union[None, list[str]] = None
        self.__includes: Union[None, list[str]] = None
        self.__already_in_session: Union[None, list[str]] = None

        self.__result: Union[None, object] = None
        self.__query_result: Union[None, QueryResult] = None
        self.__requires_retry: Union[None, bool] = None

    @property
    def query_result(self) -> QueryResult:
        return self.__query_result

    @query_result.setter
    def query_result(self, value) -> None:
        self.__query_result = value

    @property
    def result(self) -> object:
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

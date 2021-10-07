import logging

import requests

from pyravendb.commands.raven_commands import QueryCommand
from pyravendb.data.query import IndexQuery
from pyravendb.documents.session.in_memory_document_session_operations import InMemoryDocumentSessionOperations
from pyravendb.documents.session.tokens import FieldsToFetchToken


class MultiGetOperation:
    def __init__(self, session: InMemoryDocumentSessionOperations):
        self.__session = session

    def create_request(self, requests: list[requests.Request]):
        return MultiGetCommand(self.__session.request_executor, requests)

    def set_result(self, result: dict) -> None:
        pass


class QueryOperation:
    def __init__(
        self,
        session: InMemoryDocumentSessionOperations,
        index_name: str,
        index_query: IndexQuery,
        fields_to_fetch: FieldsToFetchToken,
        disable_entities_tracking: bool,
        metadata_only: bool,
        index_entries_only: bool,
        is_project_into: bool,
    ):
        self.__session = session
        self.__logger = logging.getLogger("query_operation")
        self.__index_name = index_name
        self.__index_query = index_query
        self.__metadata_only = metadata_only
        self.__index_entries_only = index_entries_only
        self.__is_project_into = is_project_into
        self.__fields_to_fetch = fields_to_fetch
        self.__no_tracking = disable_entities_tracking
        self.__assert_page_size_set()

    def create_request(self) -> QueryCommand:
        self.__session.increment_requests_count()
        self.log_query()
        return QueryCommand(self.__session, self.__index_query, self.__metadata_only, self.__index_entries_only)

    def __assert_page_size_set(self) -> None:
        if not self.__session.conventions.throw_if_query_page_size_is_not_set:
            return

        if not self.__index_query.page_size_set:
            return

        raise ValueError(
            "Attempt to query without explicitly specifying a page size. "
            "You can use .take() methods to set maximum number of results. "
            "By default the page size is set to Integer.MAX_VALUE and can cause severe performance degradation."
        )

    def log_query(self) -> None:
        self.__logger.info(
            f"Executing query {self.__index_query.query} on index {self.__index_name} in {self.__session.stor}"
        )

import logging
from typing import Union, List
from pyravendb.commands.commands_results import GetDocumentsResult
from pyravendb.commands.raven_commands import QueryCommand
from pyravendb.data.query import IndexQuery
from pyravendb.documents.commands import GetDocumentsCommand
from pyravendb.documents.commands.multi_get import GetRequest, MultiGetCommand
from pyravendb.documents.session.document_info import DocumentInfo
from pyravendb.documents.session.in_memory_document_session_operations import InMemoryDocumentSessionOperations
from pyravendb.documents.session.tokens.fields_to_fetch_token import FieldsToFetchToken


class MultiGetOperation:
    def __init__(self, session: InMemoryDocumentSessionOperations):
        self.__session = session

    def create_request(self, requests: List[GetRequest]):
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


class LoadStartingWithOperation:
    logger = logging.getLogger("load_starting_with_operation")

    def __init__(self, session: InMemoryDocumentSessionOperations):
        self.__session = session

        self.__start_with: Union[None, str] = None
        self.__matches: Union[None, str] = None
        self.__start: Union[None, int] = None
        self.__page_size: Union[None, int] = None
        self.__exclude: Union[None, str] = None
        self.__start_after: Union[None, str] = None

        self.__returned_ids: List[str] = []
        self.__results_set: bool = False
        self.__results: Union[None, GetDocumentsResult] = None

    def with_start_with(
        self, id_prefix: str, matches: str = None, start: int = 0, page_size: int = 25, exclude=None, start_after=None
    ):
        self.__start_with = id_prefix
        self.__matches = matches
        self.__start = start
        self.__page_size = page_size
        self.__exclude = exclude
        self.__start_after = start_after

    def create_request(self):
        self.__session.increment_requests_count()

        self.logger.info(
            f"Requesting documents with ids starting with '{self.__start_with}' from {self.__session.store_identifier}"
        )

        return GetDocumentsCommand(
            GetDocumentsCommand.GetDocumentsStartingWithCommandOptions(
                self.__start if self.__start else 0,  # todo: replace with more elegant solution
                self.__page_size if self.__page_size else 25,
                self.__start_with,
                self.__start_after,
                self.__matches,
                self.__exclude,
                False,
            )
        )

    def set_result(self, result: GetDocumentsResult) -> None:
        self.__results_set = True

        if self.__session.no_tracking:
            self.__results = result
            return

        for document in result.results:
            if not document:
                continue
            new_document_info = DocumentInfo.get_new_document_info(document)
            self.__session.documents_by_id.add(new_document_info)
            self.__returned_ids.append(new_document_info.key)

    def get_documents(self, object_type: type) -> List[object]:
        i = 0
        final_results = []

        if self.__session.no_tracking:
            if self.__results is None:
                raise RuntimeError("Cannot execute get_documents before operation execution")

            if not self.__results:
                return []

            final_results = [
                self.__session.track_entity(object_type, DocumentInfo.get_new_document_info(document))
                for document in self.__results.results
            ]
        else:
            final_results = [self.__get_document(object_type, key) for key in self.__returned_ids]

        return final_results

    def __get_document(self, object_type: type, key: str) -> object:
        if not key:
            return None

        if self.__session.is_deleted(key):
            return None

        doc = self.__session.documents_by_id.get(key)
        if doc:
            return self.__session.track_entity(object_type, document_info=doc)

        return None

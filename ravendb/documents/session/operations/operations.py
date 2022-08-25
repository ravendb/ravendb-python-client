import logging
from typing import Union, List, Type, TypeVar
from ravendb.documents.commands.crud import GetDocumentsCommand, GetDocumentsResult
from ravendb.documents.commands.multi_get import GetRequest, MultiGetCommand
from ravendb.documents.session.document_info import DocumentInfo
from ravendb.documents.session.in_memory_document_session_operations import InMemoryDocumentSessionOperations

_T = TypeVar("_T")


class MultiGetOperation:
    def __init__(self, session: InMemoryDocumentSessionOperations):
        self.__session = session

    def create_request(self, requests: List[GetRequest]):
        return MultiGetCommand(self.__session.advanced.request_executor, requests)

    def set_result(self, result: dict) -> None:
        pass


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
            f"Requesting documents with ids starting with '{self.__start_with}' from {self.__session.advanced.store_identifier}"
        )

        return GetDocumentsCommand.from_starts_with(
            self.__start_with, self.__start_after, self.__matches, self.__exclude, self.__start, self.__page_size, False
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
            self.__session._documents_by_id.add(new_document_info)
            self.__returned_ids.append(new_document_info.key)

    def get_documents(self, object_type: Type[_T]) -> List[_T]:
        i = 0
        final_results = []

        if self.__session.no_tracking:
            if self.__results is None:
                raise RuntimeError("Cannot execute get_documents before operation execution")

            if not self.__results:
                return []

            final_results = [
                self.__session.track_entity_document_info(object_type, DocumentInfo.get_new_document_info(document))
                for document in self.__results.results
            ]
        else:
            final_results = [self.__get_document(object_type, key) for key in self.__returned_ids]

        return final_results

    def __get_document(self, object_type: Type[_T], key: str) -> _T:
        if not key:
            return None

        if self.__session.is_deleted(key):
            return None

        doc = self.__session._documents_by_id.get(key)
        if doc:
            return self.__session.track_entity_document_info(object_type, doc)

        return None

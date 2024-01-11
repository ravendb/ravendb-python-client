from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Union, List, Type, TypeVar, Optional, Dict, Any, TYPE_CHECKING

import requests

from ravendb.http.server_node import ServerNode
from ravendb.documents.commands.multi_get import GetRequest, MultiGetCommand
from ravendb.documents.commands.revisions import GetRevisionsCommand
from ravendb.documents.session.document_info import DocumentInfo

from ravendb.primitives import constants
from ravendb.tools.utils import Utils, CaseInsensitiveDict
from ravendb.json.metadata_as_dictionary import MetadataAsDictionary
from ravendb.http.raven_command import RavenCommand
from ravendb.documents.commands.crud import GetDocumentsCommand

if TYPE_CHECKING:
    from ravendb.json.result import JsonArrayResult
    from ravendb.documents.session.document_session_operations.in_memory_document_session_operations import (
        InMemoryDocumentSessionOperations,
    )
    from ravendb.documents.commands.crud import GetDocumentsResult

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


class GetRevisionOperation:
    def __init__(self, session: InMemoryDocumentSessionOperations = None):
        if session is None:
            raise ValueError("Session cannot be None")
        self._session = session
        self._command: Optional[GetRevisionsCommand] = None
        self._result: Optional[JsonArrayResult] = None

    @classmethod
    def from_start_page(
        cls,
        session: InMemoryDocumentSessionOperations,
        id_: str,
        start: int,
        page_size: int,
        metadata_only: bool = False,
    ) -> GetRevisionOperation:
        self = cls(session)
        if id_ is None:
            raise ValueError("Id cannot be None")

        self._session = session
        self._command = GetRevisionsCommand.from_start_page(id_, start, page_size, metadata_only)
        return self

    @classmethod
    def from_before_date(
        cls, session: InMemoryDocumentSessionOperations, id_: str, before: datetime
    ) -> GetRevisionOperation:
        self = cls(session)
        self._command = GetRevisionsCommand.from_before(id_, before)
        return self

    @classmethod
    def from_change_vector(cls, session: InMemoryDocumentSessionOperations, change_vector: str) -> GetRevisionOperation:
        self = cls(session)
        self._command = GetRevisionsCommand.from_change_vector(change_vector)
        return self

    @classmethod
    def from_change_vectors(
        cls, session: InMemoryDocumentSessionOperations, change_vectors: List[str]
    ) -> GetRevisionOperation:
        self = cls(session)
        self._command = GetRevisionsCommand.from_change_vectors(change_vectors)
        return self

    def create_request(self) -> GetRevisionsCommand:
        if self._command.change_vectors is not None:
            return (
                None
                if self._session.check_if_all_change_vectors_are_already_included(self._command.change_vectors)
                else self._command
            )

        if self._command.change_vector is not None:
            return (
                None
                if self._session.check_if_all_change_vectors_are_already_included([self._command.change_vector])
                else self._command
            )

        if self._command.before is not None:
            return (
                None
                if self._session.check_if_revisions_by_date_time_before_already_included(
                    self._command.id, self._command.before
                )
                else self._command
            )

        return self._command

    def set_result(self, result: JsonArrayResult) -> None:
        self._result = result

    @property
    def command(self) -> GetRevisionsCommand:
        return self._command

    def _get_revision(self, document: Dict[str, Any], object_type: Type[_T] = dict) -> _T:
        if document is None:
            return Utils.get_default_value(object_type)

        metadata = None
        id_ = None
        if constants.Documents.Metadata.KEY in document:
            metadata = document.get(constants.Documents.Metadata.KEY)
            id_node = metadata.get(constants.Documents.Metadata.ID, None)
            if id_node is not None:
                id_ = id_node

        change_vector = None
        if metadata is not None and constants.Documents.Metadata.CHANGE_VECTOR in metadata:
            change_vector_node = metadata.get(constants.Documents.Metadata.CHANGE_VECTOR, None)
            if change_vector_node is not None:
                change_vector = change_vector_node

        entity = self._session.entity_to_json.convert_to_entity(
            object_type, id_, document, not self._session.no_tracking
        )
        document_info = DocumentInfo()
        document_info.key = id_
        document_info.change_vector = change_vector
        document_info.document = document
        document_info.metadata = metadata
        document_info.entity = entity
        self._session.documents_by_entity[entity] = document_info

        return entity

    def get_revisions_for(self, object_type: Type[_T]) -> List[_T]:
        results_count = len(self._result.results)
        results = []
        for i in range(results_count):
            document = self._result.results[i]
            results.append(self._get_revision(document, object_type))

        return results

    def get_revisions_metadata_for(self) -> List[MetadataAsDictionary]:
        results_count = len(self._result.results)
        results = []
        for i in range(results_count):
            document = self._result.results[i]

            metadata = None
            if constants.Documents.Metadata.KEY in document:
                metadata = document.get(constants.Documents.Metadata.KEY)

            results.append(MetadataAsDictionary(metadata))

        return results

    def get_revision(self, object_type: Type[_T]) -> _T:
        if self._result is None:
            revision: DocumentInfo

            if self._command.change_vectors is not None:
                for change_vector in self._command.change_vectors:
                    revision = self._session.include_revisions_by_change_vector.get(change_vector, None)
                    if revision is not None:
                        return self._get_revision(revision.document, object_type)

            if self._command.change_vector is not None and self._session.include_revisions_by_change_vector is not None:
                revision = self._session.include_revisions_by_change_vector.get(self._command.change_vector, None)
                if revision is not None:
                    return self._get_revision(revision.document, object_type)

            if self._command.before is not None and self._session.include_revisions_by_date_time_before is not None:
                dictionary_date_time_to_document = self._session.include_revisions_by_date_time_before.get(
                    self.command.id
                )
                if dictionary_date_time_to_document is not None:
                    revision = dictionary_date_time_to_document.get(self._command.before)
                    if revision is not None:
                        return self._get_revision(revision.document, object_type)

            return Utils.get_default_value(object_type)

        document = self._result.results[0]
        return self._get_revision(document, object_type)

    def get_revisions(self, object_type: Type[_T]) -> Dict[str, _T]:
        results = CaseInsensitiveDict()

        if self._result is None:
            for change_vector in self._command.change_vectors:
                revision = self._session.include_revisions_by_change_vector.get(change_vector)
                if revision is not None:
                    results[change_vector] = self._get_revision(revision.document, object_type)

            return results

        for i in range(len(self._command.change_vectors)):
            change_vector = self._command.change_vectors[i]
            if change_vector is None:
                continue

            json_dict = self._result.results[i]
            results[change_vector] = self._get_revision(json_dict, object_type)

        return results


class DocumentRevisionsCount:
    def __init__(self, revisions_count: int = None):
        self.revisions_count = revisions_count

    @classmethod
    def from_json(cls, json_dict: Dict) -> DocumentRevisionsCount:
        return cls(json_dict["RevisionsCount"])


class GetRevisionsCountOperation:
    def __init__(self, doc_id: str):
        self._doc_id = doc_id

    def create_request(self) -> RavenCommand[int]:
        return self.GetRevisionsCountCommand(self._doc_id)

    class GetRevisionsCountCommand(RavenCommand[int]):
        def __init__(self, id_: str):
            super().__init__(int)
            if id_ is None:
                raise ValueError("Id cannot be None")

            self._id = id_

        def create_request(self, node: ServerNode) -> requests.Request:
            return requests.Request(
                "GET", f"{node.url}/databases/{node.database}/revisions/count?&id={Utils.quote_key(self._id)}"
            )

        def set_response(self, response: Optional[str], from_cache: bool) -> None:
            if response is None:
                self.result = 0
                return

            self.result = DocumentRevisionsCount.from_json(json.loads(response)).revisions_count

        def is_read_request(self) -> bool:
            return True

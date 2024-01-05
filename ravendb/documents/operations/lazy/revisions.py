import datetime
import json
from enum import Enum
from typing import TYPE_CHECKING, Type, TypeVar, List, Optional, Dict

from ravendb.documents.commands.multi_get import GetRequest, GetResponse
from ravendb.documents.operations.lazy.definition import LazyOperation
from ravendb.documents.session.operations.operations import GetRevisionOperation
from ravendb.json.result import JsonArrayResult
from ravendb.json.metadata_as_dictionary import MetadataAsDictionary

if TYPE_CHECKING:
    from ravendb.documents.session.document_session import DocumentSession
    from ravendb.documents.queries.query import QueryResult
    from ravendb.documents.store.lazy import Lazy


_T = TypeVar("_T")


class LazyRevisionOperations:
    def __init__(self, delegate: "DocumentSession"):
        self._delegate = delegate

    def get_by_change_vector(self, change_vector: str, object_type: Type[_T]) -> "Lazy[_T]":
        operation = GetRevisionOperation.from_change_vector(self._delegate, change_vector)
        lazy_revision_operation = LazyRevisionOperation(operation, LazyRevisionOperation.Mode.SINGLE, object_type)
        return self._delegate.add_lazy_operation(object_type, lazy_revision_operation, None)

    def get_metadata_for(
        self, id_: str, start: Optional[int] = 0, page_size: Optional[int] = 25
    ) -> "Lazy[List[MetadataAsDictionary]]":
        operation = GetRevisionOperation.from_start_page(self._delegate, id_, start, page_size)
        lazy_revision_operation = LazyRevisionOperation(
            operation, LazyRevisionOperation.Mode.LIST_OF_METADATA, MetadataAsDictionary
        )
        return self._delegate.add_lazy_operation(list, lazy_revision_operation, None)

    def get_by_change_vectors(
        self, change_vectors: List[str], object_type: Optional[Type[_T]] = None
    ) -> "Lazy[Dict[str, _T]]":
        operation = GetRevisionOperation.from_change_vectors(self._delegate, change_vectors)
        lazy_revision_operation = LazyRevisionOperation(operation, LazyRevisionOperation.Mode.MAP, object_type)
        return self._delegate.add_lazy_operation(dict, lazy_revision_operation, None)

    def get_by_before_date(
        self, id_: str, date: datetime.datetime, object_type: Optional[Type[_T]] = None
    ) -> "Lazy[_T]":
        operation = GetRevisionOperation.from_before_date(self._delegate, id_, date)
        lazy_revision_operation = LazyRevisionOperation(operation, LazyRevisionOperation.Mode.SINGLE, object_type)
        return self._delegate.add_lazy_operation(object_type, lazy_revision_operation, None)

    def get_for(
        self, id_: str, object_type: Optional[Type[_T]] = None, start: Optional[int] = 0, page_size: Optional[int] = 25
    ) -> "Lazy[List[_T]]":
        operation = GetRevisionOperation.from_start_page(self._delegate, id_, start, page_size)
        lazy_revision_operation = LazyRevisionOperation(operation, LazyRevisionOperation.Mode.MULTI, object_type)
        return self._delegate.add_lazy_operation(list, lazy_revision_operation, None)


class LazyRevisionOperation(LazyOperation):
    class Mode(Enum):
        SINGLE = "Single"
        MULTI = "Multi"
        MAP = "Map"
        LIST_OF_METADATA = "ListOfMetadata"

    def __init__(
        self, get_revision_operation: GetRevisionOperation, mode: Mode, object_type: Optional[Type[_T]] = None
    ):
        self.result: Optional[object] = None
        self.query_result: Optional[QueryResult] = None
        self._requires_retry: Optional[bool] = None

        self._object_type = object_type
        self._get_revision_operation = get_revision_operation
        self._mode = mode

    @property
    def requires_retry(self) -> Optional[bool]:
        return self._requires_retry

    @requires_retry.setter
    def requires_retry(self, value: bool):
        self._requires_retry = value

    def create_request(self) -> Optional[GetRequest]:
        get_revisions_command = self._get_revision_operation.command
        sb = ["?"]
        get_revisions_command.get_request_query_string(sb)
        get_request = GetRequest()
        get_request.method = "GET"
        get_request.url = "/revisions"
        get_request.query = "".join(sb)
        return get_request

    def handle_response(self, response: GetResponse) -> None:
        if response.result is None:
            return
        response_as_dict = json.loads(response.result)

        if response_as_dict is None:
            return
        json_array = response_as_dict.get("Results")

        json_array_result = JsonArrayResult()
        json_array_result.results = json_array
        self._get_revision_operation.set_result(json_array_result)

        if self._mode == LazyRevisionOperation.Mode.SINGLE:
            self.result = self._get_revision_operation.get_revision(self._object_type)
        elif self._mode == LazyRevisionOperation.Mode.MULTI:
            self.result = self._get_revision_operation.get_revisions_for(self._object_type)
        elif self._mode == LazyRevisionOperation.Mode.MAP:
            self.result = self._get_revision_operation.get_revisions(self._object_type)
        elif self._mode == LazyRevisionOperation.Mode.LIST_OF_METADATA:
            self.result = self._get_revision_operation.get_revisions_metadata_for()
        else:
            raise ValueError(f"Invalid mode: {self._mode}")

from __future__ import annotations
import enum
import json
from typing import Optional, List, Dict, TYPE_CHECKING, Union, Tuple

import requests

from ravendb.documents.operations.definitions import IOperation
from ravendb.http.http_cache import HttpCache
from ravendb.http.raven_command import RavenCommand, ServerNode
from ravendb.tools.utils import Utils

if TYPE_CHECKING:
    from ravendb.documents.store.definition import DocumentStore
    from ravendb.documents.conventions import DocumentConventions


class CounterOperationType(enum.Enum):
    NONE = "None"
    INCREMENT = "Increment"
    DELETE = "Delete"
    GET = "Get"
    PUT = "Put"


class CounterOperation:
    def __init__(
        self,
        counter_name: str,
        counter_operation_type: CounterOperationType,
        delta: Optional[int] = None,
    ):
        if not counter_name:
            raise ValueError("Missing counter_name property")
        if not counter_operation_type:
            raise ValueError(f"Missing counter_operation_type property in counter {counter_name}")
        if delta is None and counter_operation_type == CounterOperationType.INCREMENT:
            raise ValueError(f"Missing delta property in counter {counter_name} of type {counter_operation_type}")
        self.counter_name = counter_name
        self.delta = delta
        self.counter_operation_type = counter_operation_type
        self._change_vector = None
        self._document_id = None

    @classmethod
    def create(cls, counter_name: str, counter_operation_type: CounterOperationType, delta: Optional[int] = None):
        return cls(counter_name, counter_operation_type, delta)

    def to_json(self):
        return {
            "Type": self.counter_operation_type.value,
            "CounterName": self.counter_name,
            "Delta": self.delta,
        }


class DocumentCountersOperation:
    def __init__(self, document_id: str, operations: List[CounterOperation]):
        if not document_id:
            raise ValueError("Missing document_id property")

        self._document_id = document_id
        self._operations = operations

    def add_operations(self, operation: CounterOperation):
        if self._operations is None:
            self._operations = []

        self._operations.append(operation)

    @property
    def operations(self):
        return self._operations

    def to_json(self):
        if not self._operations:
            raise ValueError("Missing operations property on Counters")
        return {
            "DocumentId": self._document_id,
            "Operations": [operation.to_json() for operation in self._operations],
        }


class CounterBatch:
    def __init__(
        self,
        reply_with_all_nodes_values: Optional[bool] = None,
        documents: Optional[List[DocumentCountersOperation]] = None,
        from_etl: Optional[bool] = None,
    ):
        self.reply_with_all_nodes_values = reply_with_all_nodes_values
        self.documents = documents
        self.from_etl = from_etl

    def to_json(self) -> Dict:
        return {
            "ReplyWithAllNodesValues": self.reply_with_all_nodes_values,
            "Documents": [document_counters_operation.to_json() for document_counters_operation in self.documents],
            "FromEtl": self.from_etl,
        }


class CounterDetail:
    def __init__(
        self,
        document_id: Optional[str] = None,
        counter_name: Optional[str] = None,
        total_value: Optional[int] = None,
        etag: Optional[int] = None,
        counter_values: Optional[Dict[str, int]] = None,
    ):
        self.document_id = document_id
        self.counter_name = counter_name
        self.total_value = total_value
        self.etag = etag
        self.counter_values = counter_values

    def to_json(self) -> Dict:
        return {
            "DocumentId": self.document_id,
            "CounterName": self.counter_name,
            "TotalValue": self.total_value,
            "Etag": self.etag,
            "CounterValues": self.counter_values,
        }

    @classmethod
    def from_json(cls, json_dict: Dict) -> Optional[CounterDetail]:
        return (
            cls(
                json_dict["DocumentId"],
                json_dict["CounterName"],
                json_dict["TotalValue"],
                json_dict.get("Etag", None),
                json_dict["CounterValues"],
            )
            if json_dict is not None
            else None
        )


class CountersDetail:
    def __init__(self, counters: List[CounterDetail]):
        self.counters = counters

    def to_json(self) -> Dict:
        return {"Counters": [counter.to_json() for counter in self.counters]}

    @classmethod
    def from_json(cls, json_dict: Dict) -> CountersDetail:
        return cls([CounterDetail.from_json(counter_detail_json) for counter_detail_json in json_dict["Counters"]])


class CounterBatchOperation(IOperation[CountersDetail]):
    def __init__(self, counter_batch: CounterBatch):
        self._counter_batch = counter_batch

    def get_command(
        self, store: "DocumentStore", conventions: "DocumentConventions", cache: HttpCache
    ) -> RavenCommand[CountersDetail]:
        return CounterBatchOperation.CounterBatchCommand(self._counter_batch)

    class CounterBatchCommand(RavenCommand[CountersDetail]):
        def __init__(self, counter_batch: CounterBatch):
            super().__init__(CountersDetail)

            if counter_batch is None:
                raise ValueError("Counter batch cannot be None")

            self._counter_batch = counter_batch

        def create_request(self, node: ServerNode) -> requests.Request:
            return requests.Request(
                "POST", f"{node.url}/databases/{node.database}/counters", data=self._counter_batch.to_json()
            )

        def set_response(self, response: Optional[str], from_cache: bool) -> None:
            if response is None:
                return

            self.result = CountersDetail.from_json(json.loads(response))

        def is_read_request(self) -> bool:
            return False


class GetCountersOperation(IOperation[CountersDetail]):
    def __init__(
        self, doc_id: str, counters: Optional[Union[str, List[str]]] = None, return_full_results: Optional[bool] = None
    ):
        self._doc_id = doc_id
        self._counters = [] if counters is None else counters if isinstance(counters, list) else [counters]
        self._return_full_results = return_full_results

    def get_command(
        self, store: "DocumentStore", conventions: "DocumentConventions", cache: HttpCache
    ) -> RavenCommand[CounterDetail]:
        return GetCountersOperation.GetCounterValuesCommand(
            self._doc_id, self._counters, self._return_full_results, conventions
        )

    class GetCounterValuesCommand(RavenCommand[CountersDetail]):
        def __init__(
            self, doc_id: str, counters: List[str], return_full_results: bool, conventions: DocumentConventions
        ):
            super(GetCountersOperation.GetCounterValuesCommand, self).__init__(CountersDetail)

            if doc_id is None:
                raise ValueError("Doc id cannot be None")

            self._doc_id = doc_id
            self._counters = counters
            self._return_full_results = return_full_results
            self._conventions = conventions

        def create_request(self, node: ServerNode) -> requests.Request:
            path_builder = [node.url]
            path_builder.append("/databases/")
            path_builder.append(node.database)
            path_builder.append("/counters?docId=")
            path_builder.append(Utils.quote_key(self._doc_id))

            request = requests.Request("GET")

            if self._counters:
                if len(self._counters) > 1:
                    request = self._prepare_request_with_multiple_counters(path_builder, request)
                else:
                    path_builder.append("&counter=")
                    path_builder.append(Utils.quote_key(self._counters[0]))

            if self._return_full_results and request.method == "GET":
                path_builder.append("&full=true")

            request.url = "".join(path_builder)

            return request

        def _prepare_request_with_multiple_counters(
            self, path_builder: List[str], request: requests.Request
        ) -> requests.Request:
            unique_names, sum_length = self._get_ordered_unique_names()

            # if it is too big, we drop to POST (note that means that we can't use the HTTP cache any longer)
            # we are fine with that, such requests are going to be rare
            if sum_length < 1024:
                for unique_name in unique_names:
                    path_builder.append("&counter=")
                    path_builder.append(Utils.quote_key(unique_name or ""))
            else:
                request = requests.Request("POST")

                doc_ops = DocumentCountersOperation(self._doc_id, [])

                for counter in unique_names:
                    counter_operation = CounterOperation(counter, CounterOperationType.GET)
                    doc_ops.operations.append(counter_operation)

                batch = CounterBatch()
                batch.documents = [doc_ops]
                batch.reply_with_all_nodes_values = self._return_full_results

                request.data = batch.to_json()
            return request

        def set_response(self, response: Optional[str], from_cache: bool) -> None:
            if response is None:
                return

            self.result = CountersDetail.from_json(json.loads(response))

        def _get_ordered_unique_names(self) -> Tuple[List[str], int]:
            unique_names = set(self._counters)
            sum_length = sum([len(counter) if counter else 0 for counter in unique_names])
            return list(unique_names), sum_length

        def is_read_request(self) -> bool:
            return True

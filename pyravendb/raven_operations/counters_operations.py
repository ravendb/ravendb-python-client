from typing import Optional, List
from pyravendb.commands.raven_commands import *
from pyravendb.tools.utils import Utils
from pyravendb.data.counters import *
from .operations import Operation
import io
import json


class CounterOperation:
    def __init__(self, counter_name: str, counter_operation_type: CounterOperationType, delta: Optional[float] = None):
        """
        delta will be optional only if counter_operation_type
        not in (CounterOperationType.put, CounterOperationType.increment)
        """

        if not counter_name:
            raise ValueError("Missing counter_name property")
        if not counter_operation_type:
            raise ValueError(f"Missing counter_operation_type property in counter {counter_name}")
        if delta is None and counter_operation_type == CounterOperationType.increment:
            raise ValueError(f"Missing delta property in counter {counter_name} of type {counter_operation_type}")
        self.counter_name = counter_name
        self.delta = delta
        self.counter_operation_type = counter_operation_type
        self._change_vector = None
        self._document_id = None

    def to_json(self):
        return {"Type": self.counter_operation_type, "CounterName": self.counter_name, "Delta": self.delta}


class DocumentCountersOperation:
    def __init__(self, document_id: str, operations: Optional[List[CounterOperation] or CounterOperation] = None):
        if not document_id:
            raise ValueError("Missing document_id property on Counters")

        self._document_id = document_id
        self._operations = [operations] if operations and not isinstance(operations, list) else operations

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
        return {"DocumentId": self._document_id, "Operations": [operation.to_json() for operation in self._operations]}


class CounterBatch:
    def __init__(self, documents: List[DocumentCountersOperation],
                 reply_with_all_nodes_values: bool = False, from_etl: bool = False):
        self._documents = documents
        self._reply_with_all_nodes_values = reply_with_all_nodes_values
        self._from_etl = from_etl

    def to_json(self):
        return {"ReplyWithAllNodesValues": self._reply_with_all_nodes_values,
                "Documents": [document.to_json() for document in self._documents],
                "FromEtl": self._from_etl}


class GetCountersOperation(Operation):

    def __init__(self, document_id: str, counters: Optional[List[str] or str] = None,
                 return_full_result: Optional[bool] = False):
        super().__init__()
        if not document_id:
            raise ValueError("Invalid document Id, please provide a valid value and try again")
        self._document_id = document_id
        if counters is None:
            counters = []
        if not isinstance(counters, list):
            counters = [counters]
        self._counters = counters
        self._return_full_result = return_full_result

    def get_command(self, store, conventions, cache=None):
        return self._GetCounterValuesCommand(self._document_id, self._counters, self._return_full_result)

    class _GetCounterValuesCommand(RavenCommand):

        def __init__(self, document_id: str, counters: List[str], return_full_result: Optional[bool] = False):
            super().__init__(method="GET", is_read_request=True)
            if not document_id:
                raise ValueError("Invalid document Id, please provide a valid value and try again")

            self._document_id = document_id
            self._counters = counters
            self._return_full_result = return_full_result

        def set_response(self, response):
            if response is None:
                return None
            try:
                response = response.json()
                if "Error" in response:
                    raise exceptions.ErrorResponseException(response["Message"], response["Type"])
            except ValueError as e:
                raise exceptions.ErrorResponseException(e)

            return response

        def create_request(self, server_node):
            self.url = (f"{server_node.url}/databases/{server_node.database}"
                        f"/counters?docId={Utils.quote_key(self._document_id)}"
                        f"{f'&full={True}' if self._return_full_result else ''}")

            stream = io.BytesIO()
            if self._counters:
                _set = set([counter for counter in self._counters if counter])
                counters_count = len(_set)
                if 1024 > counters_count > 1:
                    self.url += ''.join(f"&counter={Utils.quote_key(counter)}" for counter in _set if counter)
                elif counters_count >= 1024:
                    document_operation = DocumentCountersOperation(document_id=self._document_id)
                    for counter in _set:
                        document_operation.add_operations(
                            operation=CounterOperation(counter, counter_operation_type=CounterOperationType.get))
                    batch = CounterBatch(documents=[document_operation])
                    self.method = "POST"
                    self.use_stream = True
                    stream.write(json.dumps(batch.to_json(), default=Utils.json_default).encode())
                    stream.seek(0, 0)
                    self.data = stream
                else:
                    self.url += f"&counter={Utils.quote_key(self._counters[0])}"


class CounterBatchOperation(Operation):

    def __init__(self, counter_batch: CounterBatch):
        super().__init__()
        if not counter_batch:
            raise ValueError("counter_batch cannot be None")
        self._counter_batch = counter_batch

    def get_command(self, store, conventions, cache=None):
        return self._CounterBatchCommand(self._counter_batch)

    class _CounterBatchCommand(RavenCommand):

        def __init__(self, counter_batch):
            super().__init__(method="POST")
            self._counter_batch = counter_batch

        def set_response(self, response):
            if response is None:
                return None
            try:
                response = response.json()
                if "Error" in response:
                    raise exceptions.ErrorResponseException(response["Message"], response["Type"])
            except ValueError as e:
                raise exceptions.ErrorResponseException(e)

            return response

        def create_request(self, server_node):
            self.url = f"{server_node.url}/databases/{server_node.database}/counters"
            self.data = self._counter_batch.to_json()

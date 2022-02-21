import enum
from typing import Optional


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
        delta: Optional[float] = None,
    ):
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
        return {
            "Type": self.counter_operation_type,
            "CounterName": self.counter_name,
            "Delta": self.delta,
        }


class DocumentCountersOperation:
    def __init__(self, document_id: str, *operations: CounterOperation):
        if not document_id:
            raise ValueError("Missing document_id property on Counters")

        self._document_id = document_id
        self._operations = list(operations)

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

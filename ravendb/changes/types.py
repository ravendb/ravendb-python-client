from __future__ import annotations
from abc import ABC
from datetime import datetime
from enum import Enum
from typing import Dict

from ravendb.tools.utils import Utils


class DatabaseChange(ABC):
    pass


class DocumentChangeType(Enum):
    NONE = "None"
    PUT = "Put"
    DELETE = "Delete"
    CONFLICT = "Conflict"
    COMMON = "Common"

    def __str__(self):
        return self.value


class DocumentChange(DatabaseChange):
    def __init__(self, type_of_change: DocumentChangeType, key: str, collection_name: str, change_vector: str):
        self.type_of_change = type_of_change
        self.key = key
        self.collection_name = collection_name
        self.change_vector = change_vector

    def __str__(self):
        return self.type_of_change.value + " on " + self.key

    @classmethod
    def from_json(cls, json_dict: Dict) -> DocumentChange:
        return cls(
            DocumentChangeType(json_dict["Type"]),
            json_dict["Id"],
            json_dict["CollectionName"],
            json_dict["ChangeVector"],
        )


class IndexChangeTypes(Enum):
    NONE = "None"

    BATCH_COMPLETED = "BatchCompleted"

    INDEX_ADDED = "IndexAdded"
    INDEX_REMOVED = "IndexRemoved"

    INDEX_DEMOTED_TO_IDLE = "IndexDemotedToIdle"
    INDEX_PROMOTED_FROM_IDLE = "IndexPromotedFromIdle"

    INDEX_DEMOTED_TO_DISABLED = "IndexDemotedToDisabled"

    INDEX_MARKED_AS_ERRORED = "IndexMarkedAsErrored"

    SIDE_BY_SIDE_REPLACE = "SideBySideReplace"

    RENAMED = "Renamed"
    INDEX_PAUSED = "IndexPaused"
    LOCK_MODE_CHANGED = "LockModeChanged"
    PRIORITY_CHANGED = "PriorityChanged"

    ROLLING_INDEX_CHANGED = "RollingIndexChanged"

    def __str__(self):
        return self.value


class IndexChange(DatabaseChange):
    def __init__(self, type_of_change: IndexChangeTypes, name: str):
        self.type_of_change = type_of_change
        self.name = name

    @classmethod
    def from_json(cls, json_dict: Dict) -> IndexChange:
        return cls(IndexChangeTypes(json_dict["Type"]), json_dict["Name"])


class TimeSeriesChangeTypes(Enum):
    NONE = "None"
    PUT = "Put"
    DELETE = "Delete"
    MIXED = "Mixed"

    def __str__(self):
        return self.value


class TimeSeriesChange(DatabaseChange):
    def __init__(
        self,
        name: str,
        from_: datetime,
        to_: datetime,
        document_id: str,
        change_vector: str,
        type_of_change: TimeSeriesChangeTypes,
        collection_name: str,
    ):
        self.name = name
        self.from_ = from_
        self.to_ = to_
        self.document_id = document_id
        self.change_vector = change_vector
        self.type_of_change = type_of_change
        self.collection_name = collection_name

    @classmethod
    def from_json(cls, json_dict: Dict) -> TimeSeriesChange:
        return cls(
            json_dict["Name"],
            Utils.string_to_datetime(json_dict["From"]),
            Utils.string_to_datetime(json_dict["To"]),
            json_dict["DocumentId"],
            json_dict["ChangeVector"],
            TimeSeriesChangeTypes(json_dict["Type"]),
            json_dict["CollectionName"],
        )


class CounterChangeTypes(Enum):
    NONE = "None"
    PUT = "Put"
    DELETE = "Delete"
    INCREMENT = "Increment"

    def __str__(self):
        return self.value


class CounterChange(DatabaseChange):
    def __init__(
        self,
        name: str,
        value: int,
        document_id: str,
        collection_name: str,
        change_vector: str,
        type_of_change: CounterChangeTypes,
    ):
        self.name = name
        self.value = value
        self.document_id = document_id
        self.collection_name = collection_name
        self.change_vector = change_vector
        self.type_of_change = type_of_change

    @classmethod
    def from_json(cls, json_dict: Dict) -> CounterChange:
        return cls(
            json_dict["Name"],
            json_dict["Value"],
            json_dict["DocumentId"],
            json_dict["CollectionName"],
            json_dict["ChangeVector"],
            CounterChangeTypes(json_dict["Type"]),
        )


class OperationStatusChange(DatabaseChange):
    def __init__(self, operation_id: str, state: Dict):
        self.operation_id = operation_id
        self.state = state

    @classmethod
    def from_json(cls, json_dict: Dict) -> OperationStatusChange:
        return OperationStatusChange(json_dict["OperationId"], json_dict["State"])


class TopologyChange(DatabaseChange):
    def __init__(self, url: str, database: str):
        self.url = url
        self.database = database

    @classmethod
    def from_json(cls, json_dict: Dict) -> TopologyChange:
        return cls(json_dict["Url"], json_dict["Database"])

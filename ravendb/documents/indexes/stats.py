from __future__ import annotations
from datetime import datetime
from typing import Dict, Any

from ravendb.documents.indexes.definitions import (
    IndexState,
    IndexPriority,
    IndexLockMode,
    IndexType,
    IndexSourceType,
    IndexRunningStatus,
)
from ravendb.tools.utils import Utils


class IndexStats:
    def __init__(
        self,
        name: str = None,
        map_attempts: int = None,
        map_successes: int = None,
        map_errors: int = None,
        map_reference_attempts: int = None,
        map_reference_successes: int = None,
        map_reference_errors: int = None,
        reduce_attempts: int = None,
        reduce_successes: int = None,
        reduce_errors: int = None,
        reduce_output_collection: str = None,
        reduce_output_reference_pattern: str = None,
        pattern_references_collection_name: str = None,
        mapped_per_second_rate: float = None,
        reduced_per_second_rate: float = None,
        max_number_of_outputs_per_document: int = None,
        collections: Dict[str, CollectionStats] = None,
        last_querying_time: datetime = None,
        state: IndexState = None,
        priority: IndexPriority = None,
        created_timestamp: datetime = None,
        last_indexing_time: datetime = None,
        stale: bool = None,
        lock_mode: IndexLockMode = None,
        type: IndexType = None,
        status: IndexRunningStatus = None,
        entries_count: int = None,
        errors_count: int = None,
        source_type: IndexSourceType = None,
        is_test_index: bool = None,
    ):
        self.name = name
        self.map_attempts = map_attempts
        self.map_successes = map_successes
        self.map_errors = map_errors
        self.map_reference_attempts = map_reference_attempts
        self.map_reference_successes = map_reference_successes
        self.map_reference_errors = map_reference_errors
        self.reduce_attempts = reduce_attempts
        self.reduce_successes = reduce_successes
        self.reduce_errors = reduce_errors
        self.reduce_output_collection = reduce_output_collection
        self.reduce_output_reference_pattern = reduce_output_reference_pattern
        self.pattern_references_collection_name = pattern_references_collection_name
        self.mapped_per_second_rate = mapped_per_second_rate
        self.reduced_per_second_rate = reduced_per_second_rate
        self.max_number_of_outputs_per_document = max_number_of_outputs_per_document
        self.collections = collections
        self.last_querying_time = last_querying_time
        self.state = state
        self.priority = priority
        self.created_timestamp = created_timestamp
        self.last_indexing_time = last_indexing_time
        self.stale = stale
        self.lock_mode = lock_mode
        self.type = type
        self.status = status
        self.entries_count = entries_count
        self.errors_count = errors_count
        self.source_type = source_type
        self.is_test_index = is_test_index

    class CollectionStats:
        def __init__(
            self,
            last_processed_document_etag: int = None,
            last_processed_tombstone_etag: int = None,
            document_lag: int = None,
            tombstone_lag: int = None,
        ):
            self.last_processed_document_etag = last_processed_document_etag
            self.last_processed_tombstone_etag = last_processed_tombstone_etag
            self.document_lag = document_lag
            self.tombstone_lag = tombstone_lag

        @classmethod
        def from_json(cls, json_dict: Dict[str, Any]) -> "IndexStats.CollectionStats":
            return cls(
                json_dict["LastProcessedDocumentEtag"],
                json_dict["LastProcessedTombstoneEtag"],
                json_dict["DocumentLag"],
                json_dict["TombstoneLag"],
            )

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> IndexStats:
        return cls(
            json_dict["Name"],
            json_dict["MapAttempts"],
            json_dict["MapSuccesses"],
            json_dict["MapErrors"],
            json_dict["MapReferenceAttempts"],
            json_dict["MapReferenceSuccesses"],
            json_dict["MapReferenceErrors"],
            json_dict["ReduceAttempts"],
            json_dict["ReduceSuccesses"],
            json_dict["ReduceErrors"],
            json_dict["ReduceOutputCollection"],
            json_dict["ReduceOutputReferencePattern"],
            json_dict["PatternReferencesCollectionName"],
            json_dict["MappedPerSecondRate"],
            json_dict["ReducedPerSecondRate"],
            json_dict["MaxNumberOfOutputsPerDocument"],
            {key: cls.CollectionStats.from_json(value) for key, value in json_dict["Collections"].items()},
            Utils.string_to_datetime(json_dict["LastQueryingTime"]),
            IndexState(json_dict["State"]),
            IndexPriority(json_dict["Priority"]),
            Utils.string_to_datetime(json_dict["CreatedTimestamp"]),
            Utils.string_to_datetime(json_dict["LastIndexingTime"]),
            json_dict["IsStale"],
            IndexLockMode(json_dict["LockMode"]),
            IndexType(json_dict["Type"]),
            IndexRunningStatus(json_dict["Status"]),
            json_dict["EntriesCount"],
            json_dict["ErrorsCount"],
            IndexSourceType(json_dict["SourceType"]),
            json_dict.get("IsTestIndex", None),
        )

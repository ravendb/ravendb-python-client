from __future__ import annotations
import datetime
from abc import abstractmethod
from enum import Enum


class ProjectionBehavior(Enum):
    DEFAULT = "Default"
    FROM_INDEX = "FromIndex"
    FROM_INDEX_OR_THROW = "FromIndexOrThrow"
    FROM_DOCUMENT = "FromDocument"
    FROM_DOCUMENT_OR_THROW = "FromDocumentOrThrow"


class QueryTimings:
    def __init__(self, duration_in_ms: int, timings: dict[str, QueryTimings]):
        self.duration_in_ms = duration_in_ms
        self.timings = timings

    def update(self, query_result: QueryResult) -> None:
        self.duration_in_ms = 0
        self.timings = None

        if query_result.timings is None:
            return

        self.duration_in_ms = query_result.timings.duration_in_ms
        self.timings = query_result.timings.timings


class QueryResultBase:
    @abstractmethod
    def __init__(
        self,
        results: object = None,
        includes: object = None,
        counter_includes: dict = None,
        included_counter_names: dict[str, list[str]] = None,
        time_series_includes: dict = None,
        compare_exchange_value_includes: dict = None,
        included_paths: list[str] = None,
        is_stale: bool = None,
        index_time_stamp: datetime.datetime = None,
        index_name: str = None,
        result_etag: int = None,
        last_query_time: datetime.datetime = None,
        node_tag: str = None,
        timings: QueryTimings = None,
    ):
        self.results = results
        self.includes = includes
        self.counter_includes = counter_includes
        self.included_counter_names = included_counter_names
        self.time_series_includes = time_series_includes
        self.compare_exchange_value_includes = compare_exchange_value_includes
        self.included_paths = included_paths
        self.is_stale = is_stale
        self.index_time_stamp = index_time_stamp
        self.index_name = index_name
        self.result_etag = result_etag
        self.last_query_time = last_query_time
        self.node_tag = node_tag
        self.timings = timings


class GenericQueryResult(QueryResultBase):
    def __init__(
        self,
        total_results: int = None,
        long_total_result: int = None,
        capped_max_results: int = None,
        skipped_results: int = None,
        highlightings: dict[str, dict[str, list[str]]] = None,
        explanations: dict[str, list[str]] = None,
        duration_in_ms: int = None,
        result_size: int = None,
        **kwargs,
    ):
        super(GenericQueryResult, self).__init__(**kwargs)
        self.total_results = total_results
        self.long_total_results = long_total_result
        self.capped_max_results = capped_max_results
        self.skipped_results = skipped_results
        self.highlightings = highlightings
        self.explanations = explanations
        self.duration_in_ms = duration_in_ms
        self.result_size = result_size


class QueryResult(GenericQueryResult):
    def create_snapshot(self) -> QueryResult:
        return QueryResult(
            results=self.results,
            includes=self.includes,
            index_name=self.index_name,
            index_time_stamp=self.index_time_stamp,
            included_paths=list(self.included_paths) if self.included_paths else None,
            is_stale=self.is_stale,
            skipped_results=self.skipped_results,
            total_results=self.total_results,
            long_total_result=self.long_total_results,
            highlightings=dict(self.highlightings) if self.highlightings else None,
            explanations=dict(self.explanations) if self.explanations else None,
            timings=self.timings,
            last_query_time=self.last_query_time,
            duration_in_ms=self.duration_in_ms,
            result_etag=self.result_etag,
            node_tag=self.node_tag,
            counter_includes=dict(self.counter_includes) if self.counter_includes else None,
            included_counter_names=dict(self.included_counter_names) if self.included_counter_names else None,
            time_series_includes=dict(self.time_series_includes) if self.time_series_includes else None,
            compare_exchange_value_includes=dict(self.compare_exchange_value_includes)
            if self.compare_exchange_value_includes
            else None,
        )


# todo: implement
class QueryResult:
    pass

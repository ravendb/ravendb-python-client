from __future__ import annotations
import datetime
from abc import abstractmethod
from enum import Enum
from typing import List, Dict, Optional, Union, TypeVar, Generic, TYPE_CHECKING

from ravendb.tools.utils import Utils

if TYPE_CHECKING:
    from ravendb.documents.session.tokens.query_tokens.definitions import DeclareToken, LoadToken

_TResult = TypeVar("_TResult")
_TIncludes = TypeVar("_TIncludes")


class ProjectionBehavior(Enum):
    DEFAULT = "Default"
    FROM_INDEX = "FromIndex"
    FROM_INDEX_OR_THROW = "FromIndexOrThrow"
    FROM_DOCUMENT = "FromDocument"
    FROM_DOCUMENT_OR_THROW = "FromDocumentOrThrow"


class QueryOperator(Enum):
    OR = "OR"
    AND = "AND"

    def __str__(self):
        return self.value


class QueryTimings:
    def __init__(self, duration_in_ms: int = None, timings: Dict[str, QueryTimings] = None):
        self.duration_in_ms = duration_in_ms
        self.timings = timings or {}

    def update(self, query_result: QueryResult) -> None:
        self.duration_in_ms = 0
        self.timings = None

        if query_result.timings is None:
            return

        self.duration_in_ms = query_result.timings.duration_in_ms
        self.timings = query_result.timings.timings

    @classmethod
    def from_json(cls, json_dict: Dict) -> QueryTimings:
        duration = json_dict.get("DurationInMs", None)
        timings_json = json_dict.get("Timings", None)
        if timings_json is None:
            return cls(duration, None)

        timings = {}

        for key, value in json_dict.get("Timings").items():
            timings[key] = cls.from_json(value)

        return cls(duration, timings)


class QueryData:
    def __init__(
        self,
        fields: List[str],
        projections: List[str],
        from_alias: Optional[str] = None,
        declare_tokens: Optional[List[DeclareToken]] = None,
        load_tokens: Optional[List[LoadToken]] = None,
        is_custom_function: Optional[bool] = None,
    ):
        self.fields = fields
        self.projections = projections
        self.from_alias = from_alias
        self.declare_tokens = declare_tokens
        self.load_tokens = load_tokens
        self.is_custom_function = is_custom_function

        self.map_reduce: Union[None, bool] = None
        self.project_into: Union[None, bool] = None
        self.projection_behavior: Union[None, ProjectionBehavior] = None


class QueryResultBase(Generic[_TResult, _TIncludes]):
    @abstractmethod
    def __init__(
        self,
        results: _TResult = None,
        includes: _TIncludes = None,
        counter_includes: Dict = None,
        included_counter_names: Dict[str, List[str]] = None,
        time_series_includes: Dict = None,
        compare_exchange_value_includes: Dict = None,
        included_paths: List[str] = None,
        is_stale: bool = None,
        index_timestamp: datetime.datetime = None,
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
        self.index_timestamp = index_timestamp
        self.index_name = index_name
        self.result_etag = result_etag
        self.last_query_time = last_query_time
        self.node_tag = node_tag
        self.timings = timings


class GenericQueryResult(Generic[_TResult, _TIncludes], QueryResultBase[_TResult, _TIncludes]):
    def __init__(
        self,
        results: _TResult = None,
        includes: _TIncludes = None,
        counter_includes: Dict = None,
        included_counter_names: Dict[str, List[str]] = None,
        time_series_includes: Dict = None,
        compare_exchange_value_includes: Dict = None,
        included_paths: List[str] = None,
        is_stale: bool = None,
        index_timestamp: datetime.datetime = None,
        index_name: str = None,
        result_etag: int = None,
        last_query_time: datetime.datetime = None,
        node_tag: str = None,
        timings: QueryTimings = None,
        total_results: int = None,
        long_total_result: int = None,
        capped_max_results: int = None,
        skipped_results: int = None,
        highlightings: Dict[str, Dict[str, List[str]]] = None,
        explanations: Dict[str, List[str]] = None,
        duration_in_ms: int = None,
        result_size: int = None,
    ):
        super(GenericQueryResult, self).__init__(
            results,
            includes,
            counter_includes,
            included_counter_names,
            time_series_includes,
            compare_exchange_value_includes,
            included_paths,
            is_stale,
            index_timestamp,
            index_name,
            result_etag,
            last_query_time,
            node_tag,
            timings,
        )
        self.total_results = total_results
        self.long_total_results = long_total_result
        self.capped_max_results = capped_max_results
        self.skipped_results = skipped_results
        self.highlightings = highlightings
        self.explanations = explanations
        self.duration_in_ms = duration_in_ms
        self.result_size = result_size


class QueryResult(Generic[_TResult, _TIncludes], GenericQueryResult[_TResult, _TIncludes]):
    def create_snapshot(self) -> QueryResult:
        return QueryResult(
            self.results,
            self.includes,
            dict(self.counter_includes) if self.counter_includes else None,
            dict(self.included_counter_names) if self.included_counter_names else None,
            dict(self.time_series_includes) if self.time_series_includes else None,
            dict(self.compare_exchange_value_includes) if self.compare_exchange_value_includes else None,
            list(self.included_paths) if self.included_paths else None,
            self.is_stale,
            self.index_timestamp,
            self.index_name,
            self.result_etag,
            self.last_query_time,
            self.node_tag,
            self.timings,
            self.total_results,
            self.long_total_results,
            self.capped_max_results,
            self.skipped_results,
            dict(self.highlightings) if self.highlightings else None,
            dict(self.explanations) if self.explanations else None,
            self.timings,
            self.result_size,
        )

    @classmethod
    def from_json(cls, json_dict: dict) -> QueryResult:
        return QueryResult(
            json_dict.get("Results", None),
            json_dict.get("Includes", None),
            json_dict.get("CounterIncludes", None),
            json_dict.get("IncludedCounterNames", None),
            json_dict.get("TimeSeriesIncludes", None),
            json_dict.get("CompareExchangeValueIncludes", None),
            json_dict.get("IncludedPaths", None),
            json_dict.get("IsStale", None),
            Utils.string_to_datetime(json_dict.get("IndexTimestamp", None)),
            json_dict.get("IndexName"),
            json_dict.get("ResultEtag"),
            Utils.string_to_datetime(json_dict.get("LastQueryTime", None)),
            json_dict.get("NodeTag", None),
            QueryTimings.from_json(json_dict.get("Timings", {})),
            json_dict.get("TotalResults", None),
            json_dict.get("LongTotalResults", None),
            json_dict.get("CappedMaxResults", None),
            json_dict.get("SkippedResults", None),
            json_dict.get("Highlightings", None),
            json_dict.get("Explanations", None),
            json_dict.get("DurationInMs", None),
            json_dict.get("ResultSize", None),
        )

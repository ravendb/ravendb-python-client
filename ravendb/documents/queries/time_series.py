from __future__ import annotations
from typing import Optional, Dict, Any, List, Type, TypeVar, Generic

from ravendb.documents.session.time_series import TimeSeriesEntry, TypedTimeSeriesEntry

_T_TS_Bindable = TypeVar("_T_TS_Bindable")


class TimeSeriesQueryResult:
    def __init__(self, count: Optional[int] = None):
        self.count = count

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> TimeSeriesQueryResult:
        return cls(json_dict["Count"])


class TimeSeriesRawResult(TimeSeriesQueryResult):
    def __init__(self, count: Optional[int] = None, results: Optional[List[TimeSeriesEntry]] = None):
        super().__init__(count)
        self.results = results

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> TimeSeriesQueryResult:
        return cls(
            json_dict["Count"],
            [TimeSeriesEntry.from_json(time_series_entry_json) for time_series_entry_json in json_dict["Results"]],
        )

    def as_typed_result(self, object_type: Type[_T_TS_Bindable]) -> TypedTimeSeriesRawResult[_T_TS_Bindable]:
        result = TypedTimeSeriesRawResult()
        result.count = self.count
        result.results = [time_series_entry.as_typed_entry(object_type) for time_series_entry in self.results]
        return result


class TypedTimeSeriesRawResult(TimeSeriesQueryResult, Generic[_T_TS_Bindable]):
    def __init__(
        self, count: Optional[int] = None, results: Optional[List[TypedTimeSeriesEntry[_T_TS_Bindable]]] = None
    ):
        super().__init__(count)
        self.results = results

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> TimeSeriesQueryResult:
        return cls(
            json_dict["Count"],
            [TypedTimeSeriesEntry.from_json(typed_ts_entry_json) for typed_ts_entry_json in json_dict["Results"]],
        )

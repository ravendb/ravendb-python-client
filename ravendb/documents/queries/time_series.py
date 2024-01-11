from __future__ import annotations

import datetime
from typing import Optional, Dict, Any, List, Type, TypeVar, Generic

from ravendb.documents.session.time_series import (
    TimeSeriesEntry,
    TypedTimeSeriesEntry,
    TimeSeriesValuesHelper,
    ITimeSeriesValuesBindable,
)
from ravendb.tools.utils import Utils

_T_TS_Bindable = TypeVar("_T_TS_Bindable", bound=ITimeSeriesValuesBindable)


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
        if "Count" not in json_dict:
            json_dict = json_dict["__timeSeriesQueryFunction"]
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


class TimeSeriesQueryBuilder:
    def __init__(self, query: str = None):
        self._query = query

    @property
    def query_text(self):
        return self._query

    def raw(self, query_text: str) -> TimeSeriesQueryResult:
        self._query = query_text
        return None


class TimeSeriesRangeAggregation:
    def __init__(
        self,
        count: List[int] = None,
        max: List[float] = None,
        min: List[float] = None,
        last: List[float] = None,
        first: List[float] = None,
        average: List[float] = None,
        sum: List[float] = None,
        to_date: datetime.datetime = None,
        from_date: datetime.datetime = None,
    ):
        self.count = count
        self.max = max
        self.min = min
        self.last = last
        self.first = first
        self.average = average
        self.sum = sum
        self.to_date = to_date
        self.from_date = from_date

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> TimeSeriesRangeAggregation:
        return cls(
            json_dict["Count"] if "Count" in json_dict else None,
            json_dict["Max"] if "Max" in json_dict else None,
            json_dict["Min"] if "Min" in json_dict else None,
            json_dict["Last"] if "Last" in json_dict else None,
            json_dict["First"] if "First" in json_dict else None,
            json_dict["Average"] if "Average" in json_dict else None,
            json_dict["Sum"] if "Sum" in json_dict else None,
            Utils.string_to_datetime(json_dict["To"]),
            Utils.string_to_datetime(json_dict["From"]),
        )

    def as_typed_entry(
        self, ts_bindable_object_type: Type[_T_TS_Bindable]
    ) -> TypedTimeSeriesRangeAggregation[_T_TS_Bindable]:
        typed_entry = TypedTimeSeriesRangeAggregation()

        typed_entry.from_date = self.from_date
        typed_entry.to_date = self.to_date
        typed_entry.min = (
            TimeSeriesValuesHelper.set_fields(ts_bindable_object_type, self.min, False) if self.min else None
        )
        typed_entry.max = (
            TimeSeriesValuesHelper.set_fields(ts_bindable_object_type, self.max, False) if self.max else None
        )
        typed_entry.first = (
            TimeSeriesValuesHelper.set_fields(ts_bindable_object_type, self.first, False) if self.first else None
        )
        typed_entry.last = (
            TimeSeriesValuesHelper.set_fields(ts_bindable_object_type, self.last, False) if self.last else None
        )
        typed_entry.sum = (
            TimeSeriesValuesHelper.set_fields(ts_bindable_object_type, self.sum, False) if self.sum else None
        )
        counts = [float(count) for count in self.count]
        typed_entry.count = (
            TimeSeriesValuesHelper.set_fields(ts_bindable_object_type, counts, False) if self.count else None
        )
        typed_entry.average = (
            TimeSeriesValuesHelper.set_fields(ts_bindable_object_type, self.average, False) if self.average else None
        )

        return typed_entry


class TypedTimeSeriesRangeAggregation(Generic[_T_TS_Bindable]):
    def __init__(
        self,
        count: _T_TS_Bindable = None,
        max: _T_TS_Bindable = None,
        min: _T_TS_Bindable = None,
        last: _T_TS_Bindable = None,
        first: _T_TS_Bindable = None,
        average: _T_TS_Bindable = None,
        sum: _T_TS_Bindable = None,
        to_date: datetime.datetime = None,
        from_date: datetime.datetime = None,
    ):
        self.count = count
        self.max = max
        self.min = min
        self.last = last
        self.first = first
        self.average = average
        self.sum = sum
        self.to_date = to_date
        self.from_date = from_date


class TypedTimeSeriesAggregationResult(TimeSeriesQueryResult, Generic[_T_TS_Bindable]):
    def __init__(
        self, count: Optional[int] = None, results: List[TypedTimeSeriesRangeAggregation[_T_TS_Bindable]] = None
    ):
        super(TypedTimeSeriesAggregationResult, self).__init__(count)
        self.results = results


class TimeSeriesAggregationResult(TimeSeriesQueryResult):
    def __init__(self, count: Optional[int] = None, results: Optional[List[TimeSeriesRangeAggregation]] = None):
        super().__init__(count)
        self.results = results

    def as_typed_result(
        self, ts_bindable_object_type: Type[_T_TS_Bindable]
    ) -> TypedTimeSeriesAggregationResult[_T_TS_Bindable]:
        result = TypedTimeSeriesAggregationResult()
        result.count = self.count
        result.results = [x.as_typed_entry(ts_bindable_object_type) for x in self.results]
        return result

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> TimeSeriesAggregationResult:
        if "Count" not in json_dict:
            json_dict = json_dict["__timeSeriesQueryFunction"]
        return cls(
            json_dict["Count"], [TimeSeriesRangeAggregation.from_json(result) for result in json_dict["Results"]]
        )

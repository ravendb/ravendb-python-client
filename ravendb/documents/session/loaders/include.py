from __future__ import annotations

import datetime
from typing import Set, Tuple, Dict, Union, Optional, List, TYPE_CHECKING

from ravendb.primitives import constants
from ravendb.primitives.time_series import TimeValue
from ravendb.tools.utils import CaseInsensitiveDict, CaseInsensitiveSet

from ravendb.documents.session.time_series import (
    TimeSeriesRange,
    TimeSeriesTimeRange,
    TimeSeriesRangeType,
    TimeSeriesCountRange,
)

from ravendb.documents.conventions import DocumentConventions


class IncludeBuilderBase:
    def __init__(self, conventions: DocumentConventions):
        self._next_parameter_id: int = 1
        self._conventions = conventions
        self._documents_to_include: Set[str] = set()
        self._alias: str = ""
        self._counters_to_include_by_source_path: Dict[str, Tuple[bool, Set[str]]] = CaseInsensitiveDict()
        self._time_series_to_include_by_source_alias: Dict[str, Set[TimeSeriesRange]] = {}
        self._compare_exchange_values_to_include: Set[str] = set()
        self._include_time_series_tags: Optional[bool] = None
        self._include_time_series_document: Optional[bool] = None

    @property
    def time_series_to_include(self) -> Union[None, Set["TimeSeriesRange"]]:
        if self._time_series_to_include_by_source_alias is None:
            return None
        return self._time_series_to_include_by_source_alias.get("", None)

    @property
    def counters_to_include(self) -> Union[None, Set[str]]:
        if self._counters_to_include_by_source_path is None or len(self._counters_to_include_by_source_path) == 0:
            return None
        value = self._counters_to_include_by_source_path.get("", None)
        return value[1] if value is not None else {}

    @property
    def is_all_counters(self) -> bool:
        if self._counters_to_include_by_source_path is None:
            return False
        value = self._counters_to_include_by_source_path.get("", None)
        return value[0] if value is not None else False

    @property
    def alias(self):
        return self._alias

    @property
    def counters_to_include_by_source_path(self):
        return self._counters_to_include_by_source_path

    @property
    def time_series_to_include_by_source_alias(self):
        return self._time_series_to_include_by_source_alias

    @property
    def documents_to_include(self):
        return self._documents_to_include

    @property
    def compare_exchange_values_to_include(self):
        return self._compare_exchange_values_to_include

    def _include_compare_exchange_value(self, path: str) -> None:
        if self._compare_exchange_values_to_include is None:
            self._compare_exchange_values_to_include = {}
        self._compare_exchange_values_to_include.add(path)

    def _include_counter_with_alias(self, path: str, *names: str) -> None:
        self._with_alias()
        self._include_counters(path, *names)

    def _include_documents(self, path: str):
        if not self._documents_to_include:
            self._documents_to_include = set()
        self._documents_to_include.add(path)

    def _include_counters(self, path: str, *names: str):
        if not names:
            raise ValueError("Expected at least one name")

        self._assert_not_all_and_add_new_entry_if_needed(path)
        for name in names:
            if name.isspace():
                raise ValueError("Counters(names): 'names' should not contain null or whitespace elements")
            self._counters_to_include_by_source_path.get(path)[1].add(name)

    def _include_all_counters_with_alias(self, path: str):
        self._with_alias()
        self._include_all_counters(path)

    def _include_all_counters(self, source_path: str):
        if self._counters_to_include_by_source_path is None:
            self._counters_to_include_by_source_path = CaseInsensitiveDict()
        val = self._counters_to_include_by_source_path.get(source_path)
        if val is not None and val[1] is not None:
            raise RuntimeError("You cannot use all_counters() after using counters(*names)")
        self._counters_to_include_by_source_path[source_path] = (True, None)

    def _assert_not_all_and_add_new_entry_if_needed(self, path: str):
        if self._counters_to_include_by_source_path is not None:
            val = self._counters_to_include_by_source_path.get(path, None)
            if val is not None and val[0]:
                raise RuntimeError("You cannot use counters(*names) after using all_counters()")

        if self._counters_to_include_by_source_path is None:
            self._counters_to_include_by_source_path = CaseInsensitiveDict()

        if path not in self._counters_to_include_by_source_path:
            self._counters_to_include_by_source_path[path] = (False, CaseInsensitiveSet())

    def _with_alias(self):
        if self._alias is None:
            self._alias = f"a_{self._next_parameter_id}"
            self._next_parameter_id += 1

    def _assert_valid(self, alias: str, name: str) -> None:
        if not name or name.isspace():
            raise ValueError("Name cannot be None or whitespace")

        if self.time_series_to_include_by_source_alias is not None:
            hash_set_2 = self.time_series_to_include_by_source_alias.get(alias, None)
            if hash_set_2:
                if constants.TimeSeries.ALL == name:
                    raise RuntimeError(
                        "IncludeBuilder : Cannot use 'includeAllTimeSeries' "
                        "after using 'includeTimeSeries' or 'includeAllTimeSeries'."
                    )

                if any([constants.TimeSeries.ALL == x.name for x in hash_set_2]):
                    raise RuntimeError(
                        "IncludeBuilder: Cannot use 'includeTimeSeries' or 'includeAllTimeSeries' "
                        "after using 'includeAllTimeSeries'."
                    )

    def _include_time_series_from_to(
        self, alias: str, name: str, from_date: datetime.datetime, to_date: datetime.datetime
    ):
        self._assert_valid(alias, name)

        if self.time_series_to_include_by_source_alias is None:
            self._time_series_to_include_by_source_alias = {}

        hash_set = self.time_series_to_include_by_source_alias.get(alias, None)
        if hash_set is None:
            self.time_series_to_include_by_source_alias[alias] = set()  # todo: comparer, define other set class
            hash_set = self.time_series_to_include_by_source_alias[alias]

        range_ = TimeSeriesRange(name, from_date, to_date)
        hash_set.add(range_)

    def _include_time_series_by_range_type_and_time(
        self, alias: str, name: str, type_: TimeSeriesRangeType, time: TimeValue
    ) -> None:
        self._assert_valid(alias, name)
        self._assert_valid_type(type_, time)

        if self.time_series_to_include_by_source_alias is None:
            self._time_series_to_include_by_source_alias = {}

        hash_set = self._time_series_to_include_by_source_alias.get(alias, None)
        if hash_set is None:
            hash_set = set()
            self._time_series_to_include_by_source_alias[alias] = hash_set

        time_range = TimeSeriesTimeRange(name, time, type_)
        hash_set.add(time_range)

    def _include_time_series_by_range_type_and_count(
        self, alias: str, name: str, type_: TimeSeriesRangeType, count: int
    ) -> None:
        self._assert_valid(alias, name)
        self._assert_valid_type_and_count(type_, count)

        if self.time_series_to_include_by_source_alias is None:
            self._time_series_to_include_by_source_alias = {}

        hash_set = self._time_series_to_include_by_source_alias.get(alias, None)
        if hash_set is None:
            hash_set = set()
            self._time_series_to_include_by_source_alias[alias] = hash_set

        count_range = TimeSeriesCountRange(name, count, type_)
        hash_set.add(count_range)

    def _include_array_of_time_series_by_range_type_and_count(
        self, names: List[str], type_: TimeSeriesRangeType, count: int
    ) -> None:
        if names is None:
            raise ValueError("Names cannot be None")

        for name in names:
            self._include_time_series_by_range_type_and_count("", name, type_, count)

    def _include_array_of_time_series_by_range_type_and_time(
        self, names: List[str], type_: TimeSeriesRangeType, time: TimeValue
    ) -> None:
        if names is None:
            raise ValueError("Names cannot be None")

        for name in names:
            self._include_time_series_by_range_type_and_time("", name, type_, time)

    @staticmethod
    def _assert_valid_type_and_count(type_: TimeSeriesRangeType, count: int) -> None:
        if type_ == TimeSeriesRangeType.NONE:
            raise ValueError("Time range type cannot be set to NONE when count is specified.")
        elif type_ == TimeSeriesRangeType.LAST:
            if count <= 0:
                raise ValueError("Count have to be positive")
        else:
            raise ValueError(f"Not supported time range type {type_.value}")

    @staticmethod
    def _assert_valid_type(type_: TimeSeriesRangeType, time: TimeValue) -> None:
        if type_ == TimeSeriesRangeType.NONE:
            raise ValueError("Time range type cannot be set to NONE when time is specified.")
        elif type_ == TimeSeriesRangeType.LAST:
            if time is not None:
                if time.value <= 0:
                    raise ValueError("Time range type cannot be set to LAST when time is negative or zero.")

                return

            raise ValueError("Time range type cannot be set to LAST when time is not specified.")
        else:
            raise RuntimeError(f"Not supported time range type: {type_}")


class IncludeBuilder(IncludeBuilderBase):
    def __init__(self, conventions: DocumentConventions):
        super(IncludeBuilder, self).__init__(conventions)

    def include_documents(self, path: str) -> IncludeBuilderBase:
        self._include_documents(path)
        return self

    def include_counter(self, name: str) -> IncludeBuilderBase:
        self._include_counters("", name)
        return self

    def include_counters(self, *names: str) -> IncludeBuilderBase:
        for name in names:
            self.include_counter(name)
        return self

    def include_all_counters(self) -> IncludeBuilderBase:
        self._include_all_counters("")
        return self

    def include_time_series(
        self,
        name: str,
        from_date: Optional[datetime.datetime] = None,
        to_date: Optional[datetime.datetime] = None,
        alias: Optional[str] = "",
    ) -> IncludeBuilderBase:
        self._include_time_series_from_to(alias, name, from_date, to_date)
        return self

    def include_time_series_by_range_type_and_time(
        self, name: Optional[str], type_: TimeSeriesRangeType, time: TimeValue
    ) -> IncludeBuilderBase:
        self._include_time_series_by_range_type_and_time("", name, type_, time)
        return self

    def include_time_series_by_range_type_and_count(
        self, name: str, type_: TimeSeriesRangeType, count: int
    ) -> IncludeBuilderBase:
        self._include_time_series_by_range_type_and_count("", name, type_, count)
        return self

    def include_array_of_time_series_by_range_type_and_time(
        self, names: List[str], type_: TimeSeriesRangeType, time: TimeValue
    ) -> IncludeBuilderBase:
        self._include_array_of_time_series_by_range_type_and_time(names, type_, time)
        return self

    def include_array_of_time_series_by_range_type_and_count(
        self, names: List[str], type_: TimeSeriesRangeType, count: int
    ) -> IncludeBuilderBase:
        self._include_array_of_time_series_by_range_type_and_count(names, type_, count)
        return self

    def include_all_time_series_by_time(self, type_: TimeSeriesRangeType, time: TimeValue) -> IncludeBuilderBase:
        self._include_time_series_by_range_type_and_time("", constants.TimeSeries.ALL, type_, time)
        return self

    def include_all_time_series_by_count(self, type_: TimeSeriesRangeType, count: int) -> IncludeBuilderBase:
        self._include_time_series_by_range_type_and_count("", constants.TimeSeries.ALL, type_, count)
        return self

    def include_compare_exchange_value(self, path: str) -> IncludeBuilderBase:
        super(IncludeBuilder, self)._include_compare_exchange_value(path)
        return self


class QueryIncludeBuilder(IncludeBuilderBase):
    def include_counter(self, name: str, path: Optional[str] = ""):
        self._include_counter_with_alias(path, name)
        return self

    def include_counters(self, *names: str, path: Optional[str] = ""):
        self._include_counter_with_alias(path, *names)
        return self

    def include_all_counters(self, path: Optional[str] = ""):
        self._include_all_counters_with_alias(path)
        return self

    def include_documents(self, path: str):
        self._include_documents(path)
        return self

    def include_time_series(
        self,
        name: str,
        from_date: Optional[datetime.datetime] = None,
        to_date: Optional[datetime.datetime] = None,
        alias: Optional[str] = "",
    ):
        self._include_time_series_from_to(alias, name, from_date, to_date)
        return self

    def include_time_series_range_type_(self, name):
        pass

    def include_compare_exchange_value(self, path: str):
        self._include_compare_exchange_value(path)
        return self


class SubscriptionIncludeBuilder(IncludeBuilderBase):
    def __init__(self, conventions: DocumentConventions):
        super(SubscriptionIncludeBuilder, self).__init__(conventions)

    def include_documents(self, path: str) -> SubscriptionIncludeBuilder:
        self._include_documents(path)
        return self

    def include_counter(self, name: str) -> SubscriptionIncludeBuilder:
        self._include_counters("", name)
        return self

    def include_counters(self, *names: str) -> SubscriptionIncludeBuilder:
        self._include_counters("", *names)
        return self

    def include_all_counters(self) -> SubscriptionIncludeBuilder:
        self._include_all_counters("")
        return self

    def include_time_series_by_range_type_and_time(
        self, name: str, ts_type: TimeSeriesRangeType, time: TimeValue
    ) -> SubscriptionIncludeBuilder:
        self._include_time_series_by_range_type_and_time("", name, ts_type, time)
        return self

    def include_time_series_by_range_type_and_count(
        self, name: str, ts_type: TimeSeriesRangeType, count: int
    ) -> SubscriptionIncludeBuilder:
        self._include_time_series_by_range_type_and_count("", name, ts_type, count)
        return self

    def include_all_time_series_by_range_type_and_count(
        self, ts_type: TimeSeriesRangeType, count: int
    ) -> SubscriptionIncludeBuilder:
        self._include_time_series_by_range_type_and_count("", constants.TimeSeries.ALL, ts_type, count)
        return self

    def include_all_time_series_by_range_type_and_time(
        self, ts_type: TimeSeriesRangeType, time: TimeValue
    ) -> SubscriptionIncludeBuilder:
        self._include_time_series_by_range_type_and_time("", constants.TimeSeries.ALL, ts_type, time)
        return self


class TimeSeriesIncludeBuilder(IncludeBuilderBase):
    def __init__(self, conventions: DocumentConventions):
        super(TimeSeriesIncludeBuilder, self).__init__(conventions)

    def include_tags(self) -> TimeSeriesIncludeBuilder:
        self._include_time_series_tags = True
        return self

    def include_document(self) -> TimeSeriesIncludeBuilder:
        self._include_time_series_document = True
        return self

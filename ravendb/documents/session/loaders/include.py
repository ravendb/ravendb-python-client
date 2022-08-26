from __future__ import annotations
import datetime
from typing import Set, Tuple, Dict, Union, Optional
from ravendb.documents.conventions import DocumentConventions
from ravendb.data.timeseries import TimeSeriesRange
from ravendb.tools.utils import CaseInsensitiveDict, CaseInsensitiveSet


class IncludeBuilderBase:
    def __init__(self, conventions: DocumentConventions):
        self._next_parameter_id: int = 1
        self._conventions = conventions
        self._documents_to_include: Set[str] = set()
        self._alias: str = ""
        self._counters_to_include_by_source_path: Dict[str, Tuple[bool, Set[str]]] = CaseInsensitiveDict()
        self._time_series_to_include_by_source_alias: Dict[str, Set[TimeSeriesRange]] = {}
        self._compare_exchange_values_to_include: Set[str] = set()

    @property
    def _time_series_to_include(self) -> Union[None, Set[TimeSeriesRange]]:
        if self._time_series_to_include_by_source_alias is None:
            return None
        return self._time_series_to_include_by_source_alias[""]

    @property
    def _counters_to_include(self) -> Union[None, Set[str]]:
        if self._counters_to_include_by_source_path is None or len(self._counters_to_include_by_source_path) == 0:
            return None
        value = self._counters_to_include_by_source_path.get("")
        return value[1] if value is not None else {}

    @property
    def _is_all_counters(self) -> bool:
        if self._counters_to_include_by_source_path is None:
            return False
        value = self._counters_to_include_by_source_path.get("")
        return value[0] if value is not None else False

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
            if isinstance(name, list):
                self._include_counters(path, *name)
                continue
            self._counters_to_include_by_source_path.get(path)[1].add(name)

    def _include_all_counters_with_alias(self, path: str):
        self._with_alias()
        self._include_all_counters(path)

    def _include_all_counters(self, source_path: str):
        if self._counters_to_include_by_source_path is None:
            self._counters_to_include_by_source_path = CaseInsensitiveDict()
        val = self._counters_to_include_by_source_path.get(source_path)
        if val is not None and val[1] is not None:
            raise ValueError("You cannot use all_counters() after using counters(*names)")
        self._counters_to_include_by_source_path[source_path] = (True, None)

    def _assert_not_all_and_add_new_entry_if_needed(self, path: str):
        if self._counters_to_include_by_source_path is not None:
            val = self._counters_to_include_by_source_path.get(path, None)
            if val is not None and val[0]:
                raise ValueError("You cannot use counters(*names) after using all_counters()")

        if self._counters_to_include_by_source_path is None:
            self._counters_to_include_by_source_path = CaseInsensitiveDict()

        if path not in self._counters_to_include_by_source_path:
            self._counters_to_include_by_source_path[path] = (False, CaseInsensitiveSet())

    def _with_alias(self):
        if self._alias is None:
            self._alias = f"a_{self._next_parameter_id}"
            self._next_parameter_id += 1

    # todo: more time series methods
    def _include_time_series(self, alias: str, name: str, from_date: datetime.datetime, to_date: datetime.datetime):
        if not name:
            raise ValueError("Name cannot be empty")

        if self._time_series_to_include_by_source_alias is None:
            self._time_series_to_include_by_source_alias = {}

        hash_set = self._time_series_to_include_by_source_alias.get(alias, None)
        if not hash_set:
            hash_set = set()
            self._time_series_to_include_by_source_alias[alias] = hash_set

        hash_set.add(TimeSeriesRange(name, from_date, to_date))


class IncludeBuilder(IncludeBuilderBase):
    def __init__(self, conventions: DocumentConventions):
        super(IncludeBuilder, self).__init__(conventions)

    def include_documents(self, path: str) -> IncludeBuilderBase:
        self._include_documents(path)
        return self

    def include_counter(self, *names: str) -> IncludeBuilderBase:
        self._include_counters("", *names)
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
        self._include_time_series(alias, name, from_date, to_date)
        return self

    def include_compare_exchange_value(self, path: str) -> IncludeBuilderBase:
        super(IncludeBuilder, self)._include_compare_exchange_value(path)
        return self


class QueryIncludeBuilder(IncludeBuilderBase):
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
        self._include_time_series(alias, name, from_date, to_date)
        return self

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


#   def include_time_series(
#       self,
#       name:str,
#       ts_type: TimeSeriesRangeType,
#       time: TimeValue
#   ) -> SubscriptionIncludeBuilder:
#        self._include_time_series_by_range_type_and_time("", name, ts_type, time)
#        return self
#
#   def include_time_series_by_range_type_and_count(
#       self,
#       name:str,
#       ts_type: TimeSeriesRangeType,
#       time: TimeValue
#   ) -> SubscriptionIncludeBuilder:
#       self._include_time_series_by_range_type_and_count("", name, type, count)

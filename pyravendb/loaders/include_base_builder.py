import datetime
from typing import Set, Tuple, Dict, Union
from pyravendb.data import document_conventions
from pyravendb.data.timeseries import TimeSeriesRange
from pyravendb.tools.utils import CaseInsensitiveDict, CaseInsensitiveSet


class IncludeBaseBuilder:
    __next_parameter_id: int = 1

    def __init__(self, conventions: document_conventions):
        self.__conventions: document_conventions = conventions
        self._documents_to_include: Set[str] = set()
        self._alias: str = ""
        self._counters_to_include_by_source_path: Dict[str, Tuple[bool, Set[str]]] = CaseInsensitiveDict()
        self._time_series_to_include_by_source_alias: Dict[str, Set[TimeSeriesRange]] = {}
        self._compare_exchange_values_to_include: Set[str] = set()

    @property
    def documents_to_include(self):
        return self._documents_to_include

    @property
    def alias(self) -> str:
        return self._alias

    @property
    def counters_to_include(self) -> Union[dict, None]:
        if self._counters_to_include_by_source_path is None or len(self._counters_to_include_by_source_path) == 0:
            return None
        value = self._counters_to_include_by_source_path.get("")
        return value[1] if value else {}

    @property
    def counters_to_include_by_source_path(self) -> Union[dict, None]:
        return self._counters_to_include_by_source_path

    @property
    def is_all_counters(self) -> bool:
        if self._counters_to_include_by_source_path is None:
            return False
        value = self._counters_to_include_by_source_path.get("")
        return value[0] if value else False

    @property
    def time_series_to_include(self):
        if (
            self._time_series_to_include_by_source_alias is None
            or len(self._time_series_to_include_by_source_alias) == 0
        ):
            return None
        return self._time_series_to_include_by_source_alias[""]

    @property
    def time_series_to_include_by_source_alias(self):
        return self._time_series_to_include_by_source_alias

    @property
    def compare_exchange_values_to_include(self):
        return self._compare_exchange_values_to_include

    def _include_compare_exchange_value(self, path: str):
        if self.compare_exchange_values_to_include is None:
            self._compare_exchange_values_to_include = {}
        self.compare_exchange_values_to_include.add(path)

    def _include_counter_with_alias(self, path: str, *names: str):
        self._with_alias()
        self._include_counters(path, *names)

    def _include_documents(self, path: str):
        if not self.documents_to_include:
            self._documents_to_include = set()
        self._documents_to_include.add(path)

    def _include_counters(self, path: str, *names: str):
        if names is None or len(names) < 1:
            raise ValueError("Names cannot be None")
        self.assert_not_all_and_add_new_entry_if_needed(path)
        for name in names:
            if not name:
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
        if val and val[1]:
            raise ValueError("You cannot use allCounters() after using counter(name) or counters(names)")
        self._counters_to_include_by_source_path.update({source_path: (True, None)})

    def assert_not_all_and_add_new_entry_if_needed(self, path: str):
        if self._counters_to_include_by_source_path:
            val = self._counters_to_include_by_source_path.get(path, None)
            if val and val[0]:
                raise ValueError("You cannot use counter(name) after using allCounters()")

        if self._counters_to_include_by_source_path is None:
            self._counters_to_include_by_source_path = CaseInsensitiveDict()

        if path not in self._counters_to_include_by_source_path:
            self._counters_to_include_by_source_path.update({path: (False, CaseInsensitiveSet())})

    def _with_alias(self):
        if self.alias is None:
            self.__next_parameter_id += 1
            self._alias = f"a_{self.__next_parameter_id}"

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

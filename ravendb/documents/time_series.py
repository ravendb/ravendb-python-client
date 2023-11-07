from __future__ import annotations
import datetime
from typing import TYPE_CHECKING, Optional, Type, TypeVar, List

from ravendb.documents.conventions import DocumentConventions
from ravendb.documents.session.time_series import TimeSeriesValuesHelper
from ravendb.documents.operations.time_series import (
    ConfigureTimeSeriesValueNamesOperation,
    TimeSeriesPolicy,
    ConfigureTimeSeriesPolicyOperation,
    RawTimeSeriesPolicy,
    ConfigureRawTimeSeriesPolicyOperation,
    RemoveTimeSeriesPolicyOperation,
)

_T_Collection = TypeVar("_T_Collection")
_T_Time_Series_Entry = TypeVar("_T_Time_Series_Entry")

if TYPE_CHECKING:
    from ravendb import DocumentStore


def time_series_value(idx: int, name: Optional[str] = ""):
    if -127 > idx or idx > 127:
        raise ValueError("idx must be between 0 and 127")

    def decorator(field):
        field.idx = idx
        field.name = name
        return field

    return decorator


class TimeSeriesOperations:
    def __init__(self, store: "DocumentStore", database: Optional[str] = None):
        self._store = store
        self._database = database or store.database
        self._executor = self._store.maintenance.for_database(database)

    def register_type(
        self,
        collection_class: Type[_T_Collection],
        time_series_entry_class: Type[_T_Time_Series_Entry],
        name: Optional[str] = None,
    ):
        if name is None:
            name = self.get_time_series_name(time_series_entry_class, self._store.conventions)

        mapping = TimeSeriesValuesHelper.get_fields_mapping(time_series_entry_class)
        if mapping is None:
            raise RuntimeError(
                f"{self.get_time_series_name(time_series_entry_class, self._store.conventions)} "
                f"must contain @{time_series_value.__name__} decorated fields"
            )

        collection = self._store.conventions.find_collection_name(collection_class)
        value_names = [item[1] for item in mapping.values()]
        self.register(collection, name, value_names)

    def register(self, collection: str, name: str, value_names: List[str]) -> None:
        parameters = ConfigureTimeSeriesValueNamesOperation.Parameters(collection, name, value_names, True)
        command = ConfigureTimeSeriesValueNamesOperation(parameters)
        self._executor.send(command)

    def set_policy(
        self,
        collection_class: Type[_T_Collection],
        name: str,
        aggregation: datetime.timedelta,
        retention: datetime.timedelta,
    ):
        collection = self._store.conventions.find_collection_name(collection_class)
        self.set_policy_collection_name(collection, name, aggregation, retention)

    def set_policy_collection_name(
        self, collection: str, name: str, aggregation: datetime.timedelta, retention: datetime.timedelta
    ):
        p = TimeSeriesPolicy(name, aggregation, retention)
        self._executor.send(ConfigureTimeSeriesPolicyOperation(collection, p))

    def set_raw_policy(self, collection_class: Type[_T_Collection], retention: datetime.timedelta) -> None:
        collection = self._store.conventions.find_collection_name(collection_class)
        self.set_raw_policy_collection_name(collection, retention)

    def set_raw_policy_collection_name(self, collection: str, retention: datetime.timedelta) -> None:
        p = RawTimeSeriesPolicy(retention)
        self._executor.send(ConfigureRawTimeSeriesPolicyOperation(collection, p))

    def remove_policy(self, collection_class: Type[_T_Collection], name: str) -> None:
        collection = self._store.conventions.find_collection_name(collection_class)
        self.remove_policy_collection_name(collection, name)

    def remove_policy_collection_name(self, collection: str, name: str) -> None:
        self._executor.send(RemoveTimeSeriesPolicyOperation(collection, name))

    @staticmethod
    def get_time_series_name(object_type: Type[_T_Time_Series_Entry], conventions: DocumentConventions):
        return conventions.find_collection_name(object_type)

    def for_database(self, database: str) -> TimeSeriesOperations:
        if self._database.lower() == database.lower():
            return self

        return TimeSeriesOperations(self._store, database)

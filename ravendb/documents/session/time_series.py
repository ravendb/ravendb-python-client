from __future__ import annotations

import abc
import datetime
import inspect
import math
from enum import Enum
from typing import List, Dict, Type, Tuple, Any, TypeVar, Generic, Optional

from ravendb.primitives import constants
from ravendb.exceptions.raven_exceptions import RavenException
from ravendb.primitives.time_series import TimeValue
from ravendb.tools.utils import Utils

_T_TSBindable = TypeVar("_T_TSBindable")
_T_Values = TypeVar("_T_Values")


class AbstractTimeSeriesRange(abc.ABC):
    def __init__(self, name: str):
        self.name = name


class TimeSeriesRange(AbstractTimeSeriesRange):
    def __init__(self, name: str, from_date: Optional[datetime.datetime], to_date: Optional[datetime.datetime]):
        super().__init__(name)
        self.from_date = from_date
        self.to_date = to_date


class TimeSeriesTimeRange(AbstractTimeSeriesRange):
    def __init__(self, name: str, time: TimeValue, type: TimeSeriesRangeType):
        super().__init__(name)
        self.time = time
        self.type = type


class TimeSeriesCountRange(AbstractTimeSeriesRange):
    def __init__(self, name: str, count: int, type: TimeSeriesRangeType):
        super().__init__(name)
        self.count = count
        self.type = type


class TimeSeriesRangeType(Enum):
    NONE = "None"
    LAST = "Last"


class TypedTimeSeriesRollupEntry(Generic[_T_Values]):
    def __init__(self, object_type: Type[_T_Values], timestamp: datetime.datetime):
        self._object_type = object_type
        self.tag: Optional[str] = None
        self.rollup = True
        self.timestamp = timestamp

        self._first: Optional[_T_Values] = None
        self._last: Optional[_T_Values] = None
        self._max: Optional[_T_Values] = None
        self._min: Optional[_T_Values] = None
        self._sum: Optional[_T_Values] = None
        self._count: Optional[_T_Values] = None
        self._average: Optional[_T_Values] = None

    def _create_instance(self) -> _T_Values:
        try:
            return Utils.try_get_new_instance(self._object_type)  # create object type instance
        except Exception as e:
            raise RavenException(f"Unable to create instance of class: {self._object_type.__name__}", e)

    @property
    def max(self) -> _T_Values:
        if self._max is None:
            self._max = self._create_instance()
        return self._max

    @property
    def min(self) -> _T_Values:
        if self._min is None:
            self._min = self._create_instance()
        return self._min

    @property
    def count(self) -> _T_Values:
        if self._count is None:
            self._count = self._create_instance()
        return self._count

    @property
    def first(self) -> _T_Values:
        if self._first is None:
            self._first = self._create_instance()
        return self._first

    @property
    def last(self) -> _T_Values:
        if self._last is None:
            self._last = self._create_instance()
        return self._last

    @property
    def sum(self) -> _T_Values:
        if self._sum is None:
            self._sum = self._create_instance()
        return self._sum

    @property
    def average(self) -> _T_Values:
        if self._average is not None:
            return self._average

        values_count = len(TimeSeriesValuesHelper.get_fields_mapping(self._object_type))

        sums = TimeSeriesValuesHelper.get_values(self._object_type, self._sum)
        counts = TimeSeriesValuesHelper.get_values(self._object_type, self._count)
        averages: List[Optional[float]] = [None] * values_count

        for i in range(values_count):
            if counts[i] < constants.min_normal:
                averages[i] = constants.nan_value
            else:
                averages[i] = sums[i] / counts[i]

        self._average = TimeSeriesValuesHelper.set_fields(self._object_type, averages)

        return self._average

    def get_values_from_members(self) -> List[float]:
        values_count = len(TimeSeriesValuesHelper.get_fields_mapping(self._object_type))

        result: List[Optional[float]] = [None] * values_count * 6
        self._assign_rollup(result, self._first, 0)
        self._assign_rollup(result, self._last, 1)
        self._assign_rollup(result, self._min, 2)
        self._assign_rollup(result, self._max, 3)
        self._assign_rollup(result, self._sum, 4)
        self._assign_rollup(result, self._count, 5)

        return result

    def _assign_rollup(self, target: List[float], source: _T_Values, offset: int) -> None:
        if source is not None:
            values = TimeSeriesValuesHelper.get_values(self._object_type, source)
            if values is not None:
                for i in range(len(values)):
                    target[i * 6 + offset] = values[i]

    @classmethod
    def from_entry(
        cls, ts_bindable_object_type: Type[_T_TSBindable], entry: TimeSeriesEntry
    ) -> TypedTimeSeriesRollupEntry[_T_TSBindable]:
        result = TypedTimeSeriesRollupEntry(ts_bindable_object_type, entry.timestamp)
        result.rollup = True
        result.tag = entry.tag

        values = entry.values

        result._first = TimeSeriesValuesHelper.set_fields(ts_bindable_object_type, cls.extract_values(values, 0))
        result._last = TimeSeriesValuesHelper.set_fields(ts_bindable_object_type, cls.extract_values(values, 1))
        result._min = TimeSeriesValuesHelper.set_fields(ts_bindable_object_type, cls.extract_values(values, 2))
        result._max = TimeSeriesValuesHelper.set_fields(ts_bindable_object_type, cls.extract_values(values, 3))
        result._sum = TimeSeriesValuesHelper.set_fields(ts_bindable_object_type, cls.extract_values(values, 4))
        result._count = TimeSeriesValuesHelper.set_fields(ts_bindable_object_type, cls.extract_values(values, 5))

        return result

    @staticmethod
    def extract_values(input: List[float], offset: int) -> List[float]:
        length = math.ceil((len(input) - offset) / 6.0)
        idx = 0
        result: List[Optional[float]] = [None] * length

        while idx < length:
            result[idx] = input[offset + idx * 6]
            idx += 1

        return result


class TypedTimeSeriesEntry(Generic[_T_TSBindable]):
    def __init__(
        self,
        timestamp: datetime.datetime = None,
        tag: str = None,
        values: List[int] = None,
        is_rollup: bool = None,
        value: _T_TSBindable = None,
    ):
        self.timestamp = timestamp
        self.tag = tag
        self.values = values
        self.is_rollup = is_rollup
        self.value = value

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]):
        return cls(
            Utils.string_to_datetime(json_dict["Timestamp"]),
            json_dict["Tag"],
            json_dict["Values"],
            json_dict["IsRollup"],
        )


class TimeSeriesEntry:
    def __init__(
        self, timestamp: datetime.datetime = None, tag: str = None, values: List[int] = None, rollup: bool = None
    ):
        self.timestamp = timestamp
        self.tag = tag
        self.values = values
        self.rollup = rollup

    @property
    def value(self):
        if len(self.values) == 1:
            return self.values[0]
        raise ValueError("Entry has more than one value.")

    @value.setter
    def value(self, value: int):
        if len(self.values) == 1:
            self.values[0] = value
            return
        raise ValueError("Entry has more than one value")

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]):
        return cls(
            Utils.string_to_datetime(json_dict["Timestamp"]),
            json_dict["Tag"],
            json_dict["Values"],
            json_dict["IsRollup"],
        )

    def as_typed_entry(self, ts_bindable_object_type: Type[_T_TSBindable]) -> TypedTimeSeriesEntry[_T_TSBindable]:
        return TypedTimeSeriesEntry(
            self.timestamp,
            self.tag,
            self.values,
            self.rollup,
            TimeSeriesValuesHelper.set_fields(ts_bindable_object_type, self.values, self.rollup),
        )


class ITimeSeriesValuesBindable(abc.ABC):
    @abc.abstractmethod
    def get_time_series_mapping(self) -> Dict[int, Tuple[str, Optional[str]]]:
        # return a dictionary {index of time series value - (name of 'float' field, label)}
        # e.g. return {0 : ('heart', 'Heartrate'), 1: ('bp', 'Blood Pressure')}
        # for some class that has 'heart' and 'bp' float fields
        raise NotImplementedError()


class TimeSeriesValuesHelper:
    _cache: Dict[Type, Dict[int, Tuple[str, str]]] = {}

    @staticmethod
    def create_ts_bindable_class_instance(ts_bindable_object_type: Type[_T_TSBindable]) -> _T_TSBindable:
        init_signature = inspect.signature(ts_bindable_object_type.__init__)
        init_parameters = init_signature.parameters.values()

        # Create a dictionary of arguments with default values
        arguments = {
            param.name: param.default if param.default != param.empty else None
            for param in init_parameters
            if param.name != "self"
        }

        instance = None

        try:
            instance = ts_bindable_object_type(**arguments)
        except Exception as e:
            raise TypeError(
                f"Failed to get time series fields mapping. "
                f"Failed to resolve the class instance fields, "
                f"initializing object of class '{ts_bindable_object_type.__name__}' using default args: '{arguments}'. "
                f"Is the {ts_bindable_object_type.__name__}__init__() raising errors in some cases?",
                e,
            )
        return instance

    @staticmethod
    def get_fields_mapping(ts_bindable_object_type: Type[_T_TSBindable]) -> Optional[Dict[int, Tuple[str, str]]]:
        if ts_bindable_object_type not in TimeSeriesValuesHelper._cache:
            if not issubclass(ts_bindable_object_type, ITimeSeriesValuesBindable):
                return None

            ts_bindable_class_instance = TimeSeriesValuesHelper.create_ts_bindable_class_instance(
                ts_bindable_object_type
            )
            ts_bindable_type_mapping = ts_bindable_class_instance.get_time_series_mapping()

            mappings = ts_bindable_type_mapping
            mapping_keys = list(ts_bindable_type_mapping.keys())
            if mapping_keys[0] != 0 or mapping_keys[-1] != len(mapping_keys) - 1:
                raise RuntimeError(
                    f"The mapping of '{ts_bindable_object_type.__name__ }' "
                    f"must contain consecutive values starting from 0."
                )

            for key, (name, tag) in mappings.items():
                class_instance_value = ts_bindable_class_instance.__dict__[name]
                if not isinstance(class_instance_value, float) and class_instance_value is not None:
                    raise RuntimeError(
                        f"Cannot create a mapping for '{ts_bindable_object_type.__name__}' class, "
                        f"because field '{name}' is not a float"
                    )

            new_cache_entry = {}
            for idx, field_name_and_ts_value_name in ts_bindable_type_mapping.items():
                field_name = field_name_and_ts_value_name[0]
                time_series_value_name = field_name_and_ts_value_name[1] or field_name
                new_cache_entry[idx] = (field_name, time_series_value_name)

            TimeSeriesValuesHelper._cache[ts_bindable_object_type] = new_cache_entry
        return TimeSeriesValuesHelper._cache[ts_bindable_object_type]

    @staticmethod
    def get_values(ts_bindable_object_type: Type[_T_TSBindable], obj: _T_TSBindable) -> Optional[List[float]]:
        mapping = TimeSeriesValuesHelper.get_fields_mapping(ts_bindable_object_type)
        if mapping is None:
            return None

        try:
            values: List[Optional[float]] = [None] * len(mapping)
            for key, value in mapping.items():
                values[key] = obj.__dict__[value[0]]  # get field value

            return values
        except Exception as e:
            raise RavenException("Unable to read time series values", e)

    @staticmethod
    def set_fields(
        ts_bindable_object_type: Type[_T_TSBindable], values: List[float], as_rollup: bool = False
    ) -> _T_TSBindable:
        if values is None:
            return None

        mapping = TimeSeriesValuesHelper.get_fields_mapping(ts_bindable_object_type)
        if mapping is None:
            return None

        try:
            ts_bindable_class_instance = TimeSeriesValuesHelper.create_ts_bindable_class_instance(
                ts_bindable_object_type
            )
            for index, field_name_and_ts_name_tuple in mapping.items():
                value = None  # Not-a-Number

                if index < len(values):
                    if as_rollup:
                        index *= 6
                    value = values[index]

                field_name = field_name_and_ts_name_tuple[0]
                ts_bindable_class_instance.__dict__[field_name] = value

            return ts_bindable_class_instance

        except Exception as e:
            raise RavenException("Unable to read time series values.", e)

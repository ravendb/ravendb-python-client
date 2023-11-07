from __future__ import annotations
import datetime
import math
from typing import List, Dict, Type, Tuple, Any, TypeVar, Generic, Optional

from ravendb import constants
from ravendb.exceptions.raven_exceptions import RavenException
from ravendb.tools.utils import Utils

_T = TypeVar("_T")
_T_Values = TypeVar("_T_Values")


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
            raise NotImplementedError()  # create object type instance
        except Exception as e:
            raise RavenException(f"Unable to create instance of class: {self._object_type.__name__}", e)

    def get_max(self) -> _T_Values:
        if self._max is None:
            self._max = self._create_instance()
        return self._max

    def get_min(self) -> _T_Values:
        if self._min is None:
            self._min = self._create_instance()
        return self._min

    def get_count(self) -> _T_Values:
        if self._count is None:
            self._count = self._create_instance()
        return self._count

    def get_first(self) -> _T_Values:
        if self._first is None:
            self._first = self._create_instance()
        return self._first

    def get_last(self) -> _T_Values:
        if self._last is None:
            self._last = self._create_instance()
        return self._last

    def get_sum(self) -> _T_Values:
        if self._sum is None:
            self._sum = self._create_instance()
        return self._sum

    def get_average(self) -> _T_Values:
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
    def from_entry(cls, object_type: Type[_T], entry: TimeSeriesEntry):
        result = TypedTimeSeriesRollupEntry(object_type, entry.timestamp)
        result.rollup = True
        result.tag = entry.tag

        values = entry.values

        result._first = TimeSeriesValuesHelper.set_fields(object_type, cls.extract_values(values, 0))
        result._last = TimeSeriesValuesHelper.set_fields(object_type, cls.extract_values(values, 1))
        result._min = TimeSeriesValuesHelper.set_fields(object_type, cls.extract_values(values, 2))
        result._max = TimeSeriesValuesHelper.set_fields(object_type, cls.extract_values(values, 3))
        result._sum = TimeSeriesValuesHelper.set_fields(object_type, cls.extract_values(values, 4))
        result._count = TimeSeriesValuesHelper.set_fields(object_type, cls.extract_values(values, 5))

    @staticmethod
    def extract_values(input: List[float], offset: int) -> List[float]:
        length = math.ceil((len(input) - offset) / 6.0)
        idx = 0
        result: List[Optional[float]] = [None] * length

        while idx < length:
            result[idx] = input[offset + idx * 6]
            idx += 1

        return result


class TypedTimeSeriesEntry(Generic[_T]):
    def __init__(
        self,
        timestamp: datetime.datetime = None,
        tag: str = None,
        values: List[int] = None,
        rollup: bool = None,
        value: _T = None,
    ):
        self.timestamp = timestamp
        self.tag = tag
        self.values = values
        self.rollup = rollup
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

    def as_typed_entry(self, object_type: Type[_T]) -> TypedTimeSeriesEntry[_T]:
        return TypedTimeSeriesEntry(
            self.timestamp,
            self.tag,
            self.values,
            self.rollup,
            TimeSeriesValuesHelper.set_fields(object_type, self.values, self.rollup),
        )


class TimeSeriesValuesHelper:
    _cache: Dict[Type, Dict[int, Tuple[Tuple[str, Any], str]]] = {}

    @staticmethod
    def get_fields_mapping(object_type: Type) -> Dict[int, Tuple[str, str]]:
        raise NotImplementedError()  # return map of the float fields associated with time series value name

    @staticmethod
    def get_values(object_type: Type[_T], obj: _T) -> Optional[List[float]]:
        mapping = TimeSeriesValuesHelper.get_fields_mapping(object_type)
        if mapping is None:
            return None

        try:
            values: List[Optional[float]] = [None] * len(mapping)
            for key, value in mapping.items():
                values[key] = obj.__dict__()[value[0]]  # get field value

            return values
        except Exception as e:
            raise RavenException("Unable to read time series values", e)

    @staticmethod
    def set_fields(object_type: Type[_T], values: List[float], as_rollup: bool):
        if values is None:
            return None

        mapping = TimeSeriesValuesHelper.get_fields_mapping(object_type)
        if mapping is None:
            return None

        raise NotImplementedError()

    @staticmethod
    def set_fields(object_type: Type[_T], values: List[float], as_rollup: bool = False) -> _T:
        if values is None:
            return None

        mapping = TimeSeriesValuesHelper.get_fields_mapping(object_type)
        if mapping is None:
            return None

        try:
            raise NotImplementedError()

        except Exception as e:
            raise RavenException("Unable to read time series values.", e)

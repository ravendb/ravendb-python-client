from __future__ import annotations
from enum import Enum
from typing import List, Tuple, Dict, Any

from ravendb.primitives.constants import int_max, int_min


class TimeValueUnit(Enum):
    NONE = "None"
    SECOND = "Second"
    MONTH = "Month"


class TimeValue:
    SECONDS_PER_DAY = 86400
    SECONDS_IN_28_DAYS = 28 * SECONDS_PER_DAY  # lower-bound of seconds in month
    SECONDS_IN_31_DAYS = 31 * SECONDS_PER_DAY  # upper-bound of seconds in month
    SECONDS_IN_365_DAYS = 365 * SECONDS_PER_DAY  # lower-bound of seconds in year
    SECONDS_IN_366_DAYS = 366 * SECONDS_PER_DAY  # upper-bound of seconds in year

    def __init__(self, value: int, unit: TimeValueUnit):
        self.value = value
        self.unit = unit

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> TimeValue:
        return cls(json_dict["Value"], TimeValueUnit(json_dict["Unit"]))

    def to_json(self) -> Dict[str, Any]:
        return {"Value": self.value, "Unit": self.unit.value}

    def __str__(self):
        if self.value == int_max:
            return "MaxValue"
        if self.value == int_min:
            return "MinValue"
        if self.value == 0:
            return "Zero"

        if self.unit == TimeValueUnit.NONE:
            return "Unknown time unit"

        builder = []

        if self.unit == TimeValueUnit.SECOND:
            remaining_seconds = self.value

            if remaining_seconds > self.SECONDS_PER_DAY:
                days = self.value // self.SECONDS_PER_DAY
                self._append(builder, days, "day")
                remaining_seconds -= days * self.SECONDS_PER_DAY

            if remaining_seconds > 3600:
                hours = remaining_seconds // 3600
                self._append(builder, hours, "hour")
                remaining_seconds -= hours * 3600

            if remaining_seconds > 60:
                minutes = remaining_seconds // 60
                self._append(builder, minutes, "minute")
                remaining_seconds -= minutes * 60

            if remaining_seconds > 0:
                self._append(builder, remaining_seconds, "second")

        elif self.unit == TimeValueUnit.MONTH:
            if self.value >= 12:
                self._append(builder, self.value // 12, "year")
            if self.value % 12 > 0:
                self._append(builder, self.value % 12, "month")

        else:
            raise ValueError(f"Not supported unit: {self.unit}")

        return "".join(builder).strip()

    @classmethod
    def ZERO(cls):
        return cls(0, TimeValueUnit.NONE)

    @classmethod
    def MAX_VALUE(cls):
        return cls(int_max, TimeValueUnit.NONE)

    @classmethod
    def MIN_VALUE(cls):
        return cls(int_min, TimeValueUnit.NONE)

    @classmethod
    def of_seconds(cls, seconds: int) -> TimeValue:
        return cls(seconds, TimeValueUnit.SECOND)

    @classmethod
    def of_minutes(cls, minutes: int) -> TimeValue:
        return cls(minutes * 60, TimeValueUnit.SECOND)

    @classmethod
    def of_hours(cls, hours: int) -> TimeValue:
        return cls(hours * 3600, TimeValueUnit.SECOND)

    @classmethod
    def of_days(cls, days: int) -> TimeValue:
        return cls(days * cls.SECONDS_PER_DAY, TimeValueUnit.SECOND)

    @classmethod
    def of_months(cls, months: int) -> TimeValue:
        return cls(months, TimeValueUnit.MONTH)

    @classmethod
    def of_years(cls, years: int) -> TimeValue:
        return cls(12 * years, TimeValueUnit.MONTH)

    def _append(self, builder: List[str], value: int, singular: str) -> None:
        if value <= 0:
            return

        builder.append(str(value))
        builder.append(" ")
        builder.append(singular)

        if value == 1:
            builder.append(" ")
            return

        builder.append("s ")  # lucky me, no special rules here

    def _assert_seconds(self) -> None:
        if self.unit != TimeValueUnit.SECOND:
            raise ValueError("The value must be seconds")

    @staticmethod
    def _assert_valid_unit(unit: TimeValueUnit) -> None:
        if unit == TimeValueUnit.MONTH or unit == TimeValueUnit.SECOND:
            return

        raise ValueError(f"Invalid time unit: {unit}")

    @staticmethod
    def _assert_same_units(a: TimeValue, b: TimeValue) -> None:
        if a.unit != b.unit:
            raise ValueError(f"Unit isn't the same {a.unit} != {b.unit}")

    @classmethod
    def _get_bounds_in_seconds(cls, time: TimeValue) -> Tuple[int, int]:
        if time.unit == TimeValueUnit.SECOND:
            return time.value, time.value
        elif time.unit == TimeValueUnit.MONTH:
            years = time.value // 12
            upper_bound = years * cls.SECONDS_IN_366_DAYS
            lower_bound = years * cls.SECONDS_IN_365_DAYS

            remaining_months = time.value % 12
            upper_bound += remaining_months * cls.SECONDS_IN_31_DAYS
            lower_bound += remaining_months * cls.SECONDS_IN_28_DAYS

            return lower_bound, upper_bound
        else:
            raise ValueError(f"Not supported time value unit: {time.unit}")

    def compare_to(self, other: TimeValue) -> int:
        if self.value == 0 or other.value == 0:
            return self.value - other.value

        condition, result = self.is_special_compare(self, other)
        if condition:
            return result

        if self.unit == other.unit:
            return self._trim_compare_result(self.value - other.value)

        my_bounds = self._get_bounds_in_seconds(self)
        other_bounds = self._get_bounds_in_seconds(other)

        if other_bounds[1] < my_bounds[0]:
            return 1

        if other_bounds[0] > my_bounds[1]:
            return -1

        raise ValueError(f"Unable to compare {self} with {other}, since a month might have different number of days.")

    @staticmethod
    def _is_max(time: TimeValue) -> bool:
        return time.unit == TimeValueUnit.NONE and time.value == int_max

    @staticmethod
    def _is_min(time: TimeValue) -> bool:
        return time.unit == TimeValueUnit.NONE and time.value == int_min

    @staticmethod
    def _trim_compare_result(result: int) -> int:
        if result > int_max:
            return int_max

        if result < int_min:
            return int_min

        return result

    def __eq__(self, o: object):
        if id(self) == id(o):
            return True
        if o is None or self.__class__ != o.__class__:
            return False
        other: TimeValue = o
        return self.compare_to(other) == 0

    def __hash__(self):
        if self.value == 0 or self.value == int_min or self.value == int_max:
            return hash(self.value)
        return hash((self.value, self.unit))

    @classmethod
    def is_special_compare(cls, current: TimeValue, other: TimeValue) -> Tuple[bool, int]:
        result = 0

        if cls._is_max(current):
            result = 0 if cls._is_max(other) else 1
            return True, result

        if cls._is_max(other):
            result = 0 if cls._is_max(current) else -1
            return True, result

        if cls._is_min(current):
            result = 0 if cls._is_min(other) else -1
            return True, result

        if cls._is_min(other):
            result = 0 if cls._is_min(current) else 1
            return True, result

        return False, result

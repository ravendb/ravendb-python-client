from pyravendb.tools.utils import Utils
from pyravendb.tools.projection import create_entity_with_mapper
from datetime import datetime
from typing import List, Optional


class TimeSeriesRange:
    def __init__(self, name, from_date: Optional[datetime] = None, to_date: Optional[datetime] = None):
        if not name:
            raise ValueError("TimeSeriesRange must include name")
        if from_date is None:
            from_date = datetime.min
        if to_date is None:
            to_date = datetime.max
        self.name = name
        self.from_date = from_date
        self.to_date = to_date


class TimeSeriesEntry:
    def __init__(self, timestamp: datetime, tag: str, values: List[float]):
        self.timestamp = timestamp
        self.tag = tag
        self.values = values

    @property
    def value(self):
        return self.values[0]

    @staticmethod
    def create_time_series_entry(dict_obj):
        return create_entity_with_mapper(dict_obj, TimeSeriesEntry._mapper, TimeSeriesEntry, convert_to_snake_case=True)

    @staticmethod
    def _mapper(key, value):
        if key is None or value is None:
            return None

        return Utils.string_to_datetime(value) if key == "Timestamp" else value


class TimeSeriesRangeResult:
    def __init__(self, from_date: datetime, to_date: datetime, entries: List[TimeSeriesEntry],
                 total_results: int = None, hash: str = None):
        self.from_date = from_date
        self.to_date = to_date
        self.total_results = total_results
        self._hash = hash
        self.entries = entries if entries else []

    @staticmethod
    def create_time_series_range_entity(dict_obj):
        return create_entity_with_mapper(dict_obj, TimeSeriesRangeResult._mapper, TimeSeriesRangeResult,
                                         convert_to_snake_case={"From": "from_date", "To": "to_date"})

    @staticmethod
    def _mapper(key, value):
        if key is None or value is None:
            return None

        if key in ("From", "To"):
            return Utils.string_to_datetime(value)

        if key == 'Entries':
            return [TimeSeriesEntry.create_time_series_entry(entry) for entry in value]

        return value

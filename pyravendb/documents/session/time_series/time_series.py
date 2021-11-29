import datetime
from typing import List


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

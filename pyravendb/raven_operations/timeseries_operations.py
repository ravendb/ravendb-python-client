from typing import Optional, Iterable, List, Generator
from pyravendb.commands.raven_commands import *
from pyravendb.tools.utils import Utils
from .operations import Operation
from datetime import datetime

import sys


class TimeSeriesRange:
    def __init__(self, name, from_date: Optional[datetime] = None, to_date: Optional[datetime] = None):
        if not name:
            raise ValueError("TimeSeriesRange must include name")
        self.name = name
        self.from_date = from_date
        self.to_date = to_date


class GetTimeSeriesOperation(Operation):
    def __init__(self, document_id: str, ranges: Iterable[TimeSeriesRange] or TimeSeriesRange = None,
                 start: int = 0, page_size: int = sys.maxsize):
        """
        Build the time series get operations construct with ranges or from_date and to_date.
        """
        super().__init__()
        if isinstance(ranges, TimeSeriesRange):
            ranges = [ranges]

        self._ranges = ranges
        self._document_id = document_id
        self._start = start
        self._page_size = page_size

    def get_command(self, store, conventions, cache=None):
        return self._GetTimeSeriesCommand(self._document_id, self._ranges, self._start, self._page_size)

    class _GetTimeSeriesCommand(RavenCommand):
        def __init__(self, document_id: str, ranges: Iterable[TimeSeriesRange],
                     start: int = 0, page_size: int = sys.maxsize):
            super().__init__(method="GET", is_read_request=True)

            if not document_id:
                raise ValueError("Invalid document_id")

            self._document_id = document_id
            self._ranges = ranges
            self._start = start
            self._page_size = page_size

        def create_request(self, server_node):
            self.url = (f"{server_node.url}/databases/{server_node.database}"
                        f"/timeseries?id={Utils.quote_key(self._document_id)}"
                        f"{f'&start={self._start}' if self._start > 0 else ''}"
                        f"{f'&pageSize={self._page_size}' if self._page_size < sys.maxsize else ''}")

            if self._ranges is not None:
                for range_ in self._ranges:
                    self.url += (f"&name={Utils.quote_key(range_.name)}"
                                 f"&from={Utils.datetime_to_string(range_.from_date)}"
                                 f"&to={Utils.datetime_to_string(range_.to_date)}")

        def set_response(self, response):
            if response is None:
                return None
            try:
                response = response.json()
                if "Error" in response:
                    raise exceptions.ErrorResponseException(response["Error"])
            except ValueError as e:
                raise exceptions.ErrorResponseException(e)

            return response


class TimeSeriesOperation:

    def __init__(self, name: str, appends: Optional[List["RemoveOperation"]] = None,
                 removals: [List["RemoveOperation"]] = None):
        self.name = name
        self.appends = [] if not appends else Utils.sort_iterable(appends, key=lambda ao: ao.timestamp.timestamp())
        self.removals = [] if removals is None else removals

    def append(self, timestamp: datetime, values: List[float] or float, tag: Optional[str] = None):
        self.appends.append(TimeSeriesOperation.AppendOperation(timestamp, values, tag))

    def remove(self, from_date: datetime, to_date: datetime):
        self.removals.append(TimeSeriesOperation.RemoveOperation(from_date, to_date))

    def to_json(self):
        if self.appends and isinstance(self.appends, Generator):
            self.appends = next(self.appends)
        return {"Name": self.name, "Appends": [a.to_json() for a in self.appends] if self.appends else self.appends,
                "Removals": [r.to_json() for r in self.removals] if self.removals else self.removals}

    class AppendOperation:
        def __init__(self, timestamp: datetime, values: List[float] or float, tag: Optional[str] = None):
            if not timestamp:
                raise ValueError("Missing timestamp property")
            self.timestamp = timestamp
            if values is None:
                raise ValueError("Missing values property")
            try:
                if not isinstance(values, list):
                    values = [float(values)]
                else:
                    values = [float(value) for value in values]
            except ValueError:
                raise (f"One or more values from values are not numbers %s" % values)
            self.values = values
            self.tag = tag

        def to_json(self):
            _dict = {"Timestamp": Utils.datetime_to_string(self.timestamp), "Values": self.values}
            if self.tag:
                _dict["Tag"] = self.tag
            return _dict

    class RemoveOperation:
        def __init__(self, from_date: datetime, to_date: datetime):
            if not from_date:
                raise ValueError("Missing from_date property")
            if not to_date:
                raise ValueError("Missing to_date property")

            self.from_date = from_date
            self.to_date = to_date

        def to_json(self):
            return {"From": self.from_date, "To": self.to_date}


class TimeSeriesBatchOperation(Operation):
    def __init__(self, document_id: str, operation: TimeSeriesOperation):

        super().__init__()
        self._document_id = document_id
        self.__operation = operation

    def get_command(self, store, conventions, cache=None):
        return self._TimeSeriesBatchCommand(self._document_id, self.__operation)

    class _TimeSeriesBatchCommand(RavenCommand):
        def __init__(self, document_id: str, operation: TimeSeriesOperation):
            super().__init__(method="POST")

            if not document_id:
                raise ValueError("Invalid document_id")

            self._document_id = document_id
            self._operation = operation

        def create_request(self, server_node):
            self.url = (f"{server_node.url}/databases/{server_node.database}"
                        f"/timeseries?id={Utils.quote_key(self._document_id)}")

            self.data = self._operation.to_json()

        def set_response(self, response):
            if response and response.status_code != 204:
                response = response.json()
                raise exceptions.ErrorResponseException(response["Error"])

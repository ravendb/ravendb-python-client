from typing import Optional, Iterable, List
from pyravendb.data.timeseries import TimeSeriesRange
from pyravendb.commands.raven_commands import *
from pyravendb.tools.utils import Utils
from .operations import Operation
from datetime import datetime

import sys


class GetTimeSeriesOperation(Operation):
    def __init__(self, document_id: str, ranges: Iterable[TimeSeriesRange] or TimeSeriesRange,
                 start: int = 0, page_size: int = sys.maxsize):
        """
        Build the time series get operations.
        """
        super().__init__()
        if not document_id:
            raise ValueError("Invalid document Id, please provide a valid value and try again")
        if not ranges:
            raise ValueError("Ranges property Cannot be empty or None, please put a valid value and try again")

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
                raise ValueError("Invalid document Id, please provide a valid value and try again")

            if not ranges:
                raise ValueError("Ranges property Cannot be empty or None, please put a valid value and try again")

            self._document_id = document_id
            self._ranges = ranges
            self._start = start
            self._page_size = page_size

        def create_request(self, server_node):
            self.url = (f"{server_node.url}/databases/{server_node.database}"
                        f"/timeseries?docId={Utils.quote_key(self._document_id)}"
                        f"{f'&start={self._start}' if self._start > 0 else ''}"
                        f"{f'&pageSize={self._page_size}' if self._page_size < sys.maxsize else ''}")

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
                    raise exceptions.ErrorResponseException(response["Message"], response["Type"])
            except ValueError as e:
                raise exceptions.ErrorResponseException(e)

            return response


class TimeSeriesOperation:

    def __init__(self, name: str, appends: Optional[List["AppendOperation"]] = None,
                 removals: [List["RemoveOperation"]] = None):
        self.name = name
        self.appends = appends
        self.removals = removals

    def append(self, timestamp: datetime, values: List[float] or float, tag: Optional[str] = None):
        if not self.appends:
            self.appends = []
        self.appends.append(TimeSeriesOperation.AppendOperation(timestamp, values, tag))

    def remove(self, from_date: datetime = None, to_date: datetime = None):
        if not self.removals:
            self.removals = []
        self.removals.append(TimeSeriesOperation.RemoveOperation(from_date, to_date))

    def to_json(self):
        if self.appends:
            self.appends = next(Utils.sort_iterable(self.appends, key=lambda ao: ao.timestamp.timestamp()))
        return {"Name": self.name, "Appends": [a.to_json() for a in self.appends] if self.appends else self.appends,
                "Deletes": [r.to_json() for r in self.removals] if self.removals else self.removals}

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
        def __init__(self, from_date: datetime = None, to_date: datetime = None):
            self.from_date = from_date if from_date else datetime.min
            self.to_date = to_date if to_date else datetime.max

        def to_json(self):
            return {"From": Utils.datetime_to_string(self.from_date), "To": Utils.datetime_to_string(self.to_date)}


class TimeSeriesBatchOperation(Operation):
    def __init__(self, document_id: str, operation: TimeSeriesOperation):

        super().__init__()
        self._document_id = document_id
        self._operation = operation

    def get_command(self, store, conventions, cache=None):
        return self._TimeSeriesBatchCommand(self._document_id, self._operation)

    class _TimeSeriesBatchCommand(RavenCommand):
        def __init__(self, document_id: str, operation: TimeSeriesOperation):
            super().__init__(method="POST")

            if not document_id:
                raise ValueError("Invalid document_id")

            self._document_id = document_id
            self._operation = operation

        def create_request(self, server_node):
            self.url = (f"{server_node.url}/databases/{server_node.database}"
                        f"/timeseries?docId={Utils.quote_key(self._document_id)}")

            self.data = self._operation.to_json()

        def set_response(self, response):
            if response is not None and response.status_code != 204:
                response = response.json()
                raise exceptions.ErrorResponseException(response["Message"], response["Type"])


from datetime import datetime
from pyravendb.tools.utils import Utils
from pyravendb.custom_exceptions import exceptions
from pyravendb.raven_operations.timeseries_operations import GetTimeSeriesOperation
from pyravendb.data.timeseries import TimeSeriesRange, TimeSeriesEntry, TimeSeriesRangeResult
from pyravendb.raven_operations.timeseries_operations import TimeSeriesOperation
from pyravendb.commands.commands_data import TimeSeriesBatchCommandData
from typing import List, Optional
import sys


class TimeSeries:

    @staticmethod
    def raise_not_in_session(entity):
        raise ValueError(
            repr(entity) + " is not associated with the session, cannot add time-series to it. "
                           "Use document Id instead or track the entity in the session.")

    @staticmethod
    def raise_document_already_deleted_in_session(document_id, name):
        raise exceptions.InvalidOperationException(
            f"Can't modify timeseries {name} of document {document_id}, "
            f"the document was already deleted in this session.")

    def __init__(self, session, entity_or_document_id, name):
        self._session = session
        if not isinstance(entity_or_document_id, str):
            entity = self._session.documents_by_entity.get(entity_or_document_id, None)
            if not entity:
                self.raise_not_in_session(entity_or_document_id)
            entity_or_document_id = entity.get("key", None)

        if not entity_or_document_id:
            raise ValueError(entity_or_document_id)
        if not name:
            raise ValueError(name)
        self._name = name
        self._document_id = entity_or_document_id

    def _add_to_cache(self, ranges, from_date, to_date, start, page_size):
        start_date = from_date
        index = 0
        time_series_ranges = []
        # Get the missing ranges we need to fetch from the server (only if needed)
        for range_ in ranges:
            if range_.from_date <= start_date:
                if range_.to_date >= to_date:
                    return [entry for entry in range_.entries if
                            entry.timestamp >= from_date or entry.timestamp <= to_date], ranges
                else:
                    start_date = range_.to_date
                    continue
            else:
                if range_.from_date <= to_date:
                    time_series_ranges.insert(index, TimeSeriesRange(self._name, start_date, range_.from_date))
                    index += 1

                    if range_.to_date >= to_date:
                        start_date = None
                        break
                    start_date = range_.to_date
                else:
                    time_series_ranges.insert(index, TimeSeriesRange(self._name, start_date, to_date))
                    start_date = None
                    break
        if start_date:
            time_series_ranges.insert(index, TimeSeriesRange(self._name, start_date, to_date))

        self._session.increment_requests_count()

        details = self._session.advanced.document_store.operations.send(
            GetTimeSeriesOperation(self._document_id, time_series_ranges, start, page_size))

        results = [TimeSeriesRangeResult.create_time_series_range_entity(i) for i in details['Entries']]

        time_series_range_result = None
        start_from, end_from = None, None
        cache_merge = []

        for range_ in ranges:
            # Add all the entries from the ranges to the result we fetched from the server
            # build the cache again, merge ranges if needed
            if range_.from_date <= from_date and range_.to_date >= to_date or \
                    range_.from_date >= from_date and range_.to_date <= to_date or \
                    range_.from_date <= from_date and range_.to_date <= to_date and range_.to_date >= from_date or \
                    range_.from_date >= from_date and range_.from_date <= to_date and range_.to_date >= to_date:
                if time_series_range_result is None:
                    time_series_range_result = TimeSeriesRangeResult(None, None, entries=[])
                    for result in results:
                        if result.entries:
                            time_series_range_result.entries.extend(result.entries)
                time_series_range_result.entries.extend(range_.entries)
                if not start_from:
                    start_from = from_date if from_date < range_.from_date else range_.from_date
                if not end_from:
                    end_from = to_date if to_date > range_.to_date else range_.to_date
                else:
                    end_from = range_.to_date if range_.to_date > end_from else end_from
            else:
                if time_series_range_result:
                    cache_merge.append(time_series_range_result)
                cache_merge.append(range_)

        # Sort all the entries and add them to cache after we merged them.
        if time_series_range_result:
            time_series_range_result.entries = next(
                Utils.sort_iterable(time_series_range_result.entries, key=lambda ao: ao.timestamp.timestamp()))
            time_series_range_result.from_date = start_from if start_from < time_series_ranges[0].from_date else \
                time_series_ranges[0].from_date
            time_series_range_result.to_date = end_from if end_from > time_series_ranges[0].to_date else \
                time_series_ranges[-1].to_date

            cache_merge.append(time_series_range_result)

        # build the entries we need to return
        entries_to_return = []
        for entry in time_series_range_result.entries:
            if entry.timestamp >= from_date or entry.timestamp <= to_date:
                entries_to_return.append(entry)
        return entries_to_return, cache_merge

    def get(self, from_date: datetime = None, to_date: datetime = None, start: int = 0, page_size: int = sys.maxsize):

        """
        Get the timeseries in range between from_date and to_date
        (if from_date is not given will changed to datetime.min, if to_date is not given will changed to datetime.max)
        """

        document = self._session.documents_by_id.get(self._document_id, None)
        info = self._session.documents_by_entity.get(document, None) if document else None
        if info and "HasTimeSeries" not in info.get('@metadata', {}).get('@flags', {}):
            return []

        from_date = from_date if from_date else datetime.min
        to_date = to_date if to_date else datetime.max

        cache = self._session.time_series_by_document_id.get(self._document_id, None)
        ranges_result = cache.get(self._name, None) if cache else None
        if ranges_result:
            if ranges_result[0].from_date > to_date or ranges_result[-1].to_date < from_date:
                # the entire range [from_date, to_date] is out of cache bounds
                self._session.increment_requests_count()
                ranges = TimeSeriesRange(name=self._name, from_date=from_date, to_date=to_date)
                details = self._session.advanced.document_store.operations.send(
                    GetTimeSeriesOperation(self._document_id, ranges, start, page_size))

                time_series_range_result = TimeSeriesRangeResult.create_time_series_range_entity(
                    details)

                if not self._session.no_tracking:
                    index = 0 if ranges_result[0].from_date > to_date else len(ranges_result)
                    ranges_result.insert(index, time_series_range_result)
                    pass

                return time_series_range_result.entries

            entries_to_return, cache_ranges = self._add_to_cache(ranges_result, from_date, to_date, start, page_size)
            if not self._session.no_tracking:
                cache[self._name] = cache_ranges
            return entries_to_return

        self._session.increment_requests_count()
        range = TimeSeriesRange(name=self._name, from_date=from_date, to_date=to_date)
        details = self._session.advanced.document_store.operations.send(
            GetTimeSeriesOperation(self._document_id, range, start, page_size))

        time_series_range_result = TimeSeriesRangeResult.create_time_series_range_entity(
            details)
        if time_series_range_result is None:
            return []
        entries = time_series_range_result.entries
        if not self._session.no_tracking and entries:
            if cache:
                cache.update({self._name: [time_series_range_result]})
            else:
                self._session.time_series_by_document_id[self._document_id] = {self._name: [time_series_range_result]}

        return [TimeSeriesEntry.create_time_series_entry(entry) for entry in entries]

    def append(self, timestamp: datetime, values: List[float] or float, tag: Optional[str] = None):
        document = self._session.documents_by_id.get(self._document_id, None)
        if document and document in self._session.deleted_entities:
            self.raise_document_already_deleted_in_session(self._document_id, self._name)

        if not isinstance(values, list):
            values = [values]
        command = self._session.timeseries_defer_commands.get((self._document_id, "TimeSeries", self._name), None)
        if command:
            command.time_series.append(timestamp, values, tag)
        else:
            operation = TimeSeriesOperation(self._name)
            operation.append(timestamp, values, tag)
            command = TimeSeriesBatchCommandData(self._document_id, self._name, operation=operation)
            self._session.timeseries_defer_commands[(self._document_id, "TimeSeries", self._name)] = command
            self._session.defer(command)

    def remove(self, from_date: datetime, to_date: Optional[datetime] = None):
        document = self._session.documents_by_id.get(self._document_id, None)
        if document and document in self._session.deleted_entities:
            self.raise_document_already_deleted_in_session(self._document_id, self._name)

        if to_date is None:
            to_date = from_date

        command = self._session.timeseries_defer_commands.get(self._document_id, None)
        if command:
            command.time_series.remove(from_date, to_date)
        else:
            operation = TimeSeriesOperation(self._name)
            operation.remove(from_date, to_date)
            command = TimeSeriesBatchCommandData(self._document_id, self._name, operation=operation)
            self._session.timeseries_defer_commands[self._document_id] = command
            self._session.defer(command)

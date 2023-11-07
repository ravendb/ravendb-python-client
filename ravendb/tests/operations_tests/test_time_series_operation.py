from datetime import datetime, timedelta

from ravendb.documents.operations.time_series import (
    GetTimeSeriesOperation,
    TimeSeriesOperation,
    TimeSeriesBatchOperation,
)

import unittest

from ravendb.tests.test_base import TestBase, User


class TestTimeSeriesOperations(TestBase):
    def setUp(self):
        super(TestTimeSeriesOperations, self).setUp()
        self.ts_name = "Heartrate"
        with self.store.open_session() as session:
            session.store(User())
            session.save_changes()

    def tearDown(self):
        super(TestTimeSeriesOperations, self).tearDown()

    def add_time_series(self):
        ts_operation = TimeSeriesOperation(self.ts_name)

        base = datetime.now()
        base_24 = datetime(2023, 11, 14)

        ts_operation.append(TimeSeriesOperation.AppendOperation(base, [73], "heart/rates"))
        ts_operation.append(TimeSeriesOperation.AppendOperation(base + timedelta(minutes=5), [78], "heart/rates"))
        ts_operation.append(TimeSeriesOperation.AppendOperation(base_24 + timedelta(minutes=5), [789], "heart/rates"))

        time_series_batch_operation = TimeSeriesBatchOperation("users/1-A", ts_operation)
        self.store.operations.send(time_series_batch_operation)

    def test_append_new_time_series(self):
        self.add_time_series()

        # Fetch all time_series from the document
        time_series_range_result = self.store.operations.send(GetTimeSeriesOperation("users/1-A", self.ts_name))
        self.assertEqual(len(time_series_range_result.entries), 3)

    def test_remove_time_series_with_range(self):
        self.add_time_series()

        base = datetime.now()
        ts_operation = TimeSeriesOperation(self.ts_name)
        ts_operation.delete(TimeSeriesOperation.DeleteOperation(base - timedelta(days=2), base + timedelta(days=2)))

        time_series_batch_operation = TimeSeriesBatchOperation("users/1-A", ts_operation)
        self.store.operations.send(time_series_batch_operation)

        time_series_range_result = self.store.operations.send(GetTimeSeriesOperation("users/1-A", self.ts_name))
        self.assertEqual(len(time_series_range_result.entries), 1)

    def test_remove_time_series_without_range(self):
        self.add_time_series()

        time_series_operation_remove = TimeSeriesOperation(self.ts_name)
        time_series_operation_remove.delete(TimeSeriesOperation.DeleteOperation())

        time_series_batch_operation = TimeSeriesBatchOperation("users/1-A", time_series_operation_remove)
        self.store.operations.send(time_series_batch_operation)

        time_series_range_result = self.store.operations.send(GetTimeSeriesOperation("users/1-A", self.ts_name))
        self.assertIsNone(time_series_range_result)

    def test_get_time_series_with_range(self):
        self.add_time_series()

        base = datetime.now()
        from_base = base - timedelta(days=2)
        to_base = base + timedelta(days=2)

        ts_result = self.store.operations.send(GetTimeSeriesOperation("users/1-A", self.ts_name, from_base, to_base))
        self.assertEqual(len(ts_result.entries), 2)

    def test_get_time_series_without_range(self):
        self.add_time_series()

        ts_range_result = self.store.operations.send(GetTimeSeriesOperation("users/1-A", self.ts_name))
        self.assertEqual(len(ts_range_result.entries), 3)


if __name__ == "__main__":
    unittest.main()

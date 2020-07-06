from pyravendb.tests.test_base import *
from pyravendb.raven_operations.timeseries_operations import *
import unittest


class TestTimeSeriesOperations(TestBase):
    def setUp(self):
        super(TestTimeSeriesOperations, self).setUp()
        self.timeseries_range = TimeSeriesRange("Heartrate")
        with self.store.open_session() as session:
            user = User(name="Idan", age="31")
            session.store(user, key="users/1-A")
            session.save_changes()

    def tearDown(self):
        super(TestTimeSeriesOperations, self).tearDown()
        TestBase.delete_all_topology_files()

    def add_timeseries(self):
        # Add time_series to user/1-A document
        time_series_operation = TimeSeriesOperation(name="Heartrate")
        time_series_operation.append(datetime.now(), 73, tag="heart/rates")
        time_series_operation.append(datetime.now() + timedelta(minutes=5), 78, tag="heart/rates")
        time_series_operation.append(datetime(2019, 4, 23) + timedelta(minutes=5), 789, tag="heart/rates")

        time_series_batch_operation = TimeSeriesBatchOperation(document_id="users/1-A", operation=time_series_operation)
        self.store.operations.send(time_series_batch_operation)

    def test_append_new_timeseries(self):
        self.add_timeseries()
        # Fetch all time_series from the document
        time_series = self.store.operations.send(GetTimeSeriesOperation("users/1-A", ranges=self.timeseries_range))
        entries = time_series['Entries']
        self.assertEqual(len(entries), 3)

    def test_remove_timeseries_with_range(self):
        self.add_timeseries()
        time_series_operation_remove = TimeSeriesOperation(name="Heartrate")
        time_series_operation_remove.remove(from_date=datetime.now() - timedelta(days=2),
                                            to_date=datetime.now() + timedelta(days=2))

        time_series_batch_operation = TimeSeriesBatchOperation(document_id="users/1-A",
                                                               operation=time_series_operation_remove)
        self.store.operations.send(time_series_batch_operation)
        time_series = self.store.operations.send(GetTimeSeriesOperation("users/1-A", ranges=self.timeseries_range))
        entries = time_series['Entries']
        self.assertEqual(len(entries), 1)

    def test_remove_timeseries_without_range(self):
        self.add_timeseries()
        time_series_operation_remove = TimeSeriesOperation(name="Heartrate")
        time_series_operation_remove.remove()

        time_series_batch_operation = TimeSeriesBatchOperation(document_id="users/1-A",
                                                               operation=time_series_operation_remove)
        self.store.operations.send(time_series_batch_operation)
        time_series = self.store.operations.send(GetTimeSeriesOperation("users/1-A", ranges=self.timeseries_range))
        self.assertIsNone(time_series)

    def test_get_timeseries_with_range(self):
        self.add_timeseries()
        time_series_range = TimeSeriesRange("Heartrate", from_date=datetime.now() - timedelta(days=2),
                                            to_date=datetime.now() + timedelta(days=2))
        time_series = self.store.operations.send(GetTimeSeriesOperation("users/1-A", ranges=time_series_range))
        entries = time_series['Entries']
        self.assertEqual(len(entries), 2)

    def test_get_timeseries_without_range(self):
        self.add_timeseries()
        time_series_range = TimeSeriesRange("Heartrate")
        time_series = self.store.operations.send(GetTimeSeriesOperation("users/1-A", ranges=time_series_range))
        entries = time_series['Entries']
        self.assertEqual(len(entries), 3)


if __name__ == '__main__':
    unittest.main()

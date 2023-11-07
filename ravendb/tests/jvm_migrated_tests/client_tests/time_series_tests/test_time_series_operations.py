from datetime import datetime, timedelta

from ravendb.documents.operations.time_series import (
    GetTimeSeriesOperation,
    TimeSeriesOperation,
    TimeSeriesBatchOperation,
    GetTimeSeriesStatisticsOperation,
)
from ravendb.tests.test_base import TestBase, User


class TestTimeSeriesOperations(TestBase):
    def setUp(self):
        super().setUp()

    def test_can_delete_without_providing_from_and_to_dates(self):
        base_line = datetime(2023, 8, 20, 21, 30)
        doc_id = "users/ayende"

        with self.store.open_session() as session:
            session.store(User(), doc_id)

            tsf = session.time_series_for(doc_id, "HeartRate")
            tsf2 = session.time_series_for(doc_id, "BloodPressure")
            tsf3 = session.time_series_for(doc_id, "BodyTemperature")

            for j in range(100):
                tsf.append_single(base_line + timedelta(minutes=j), j)
                tsf2.append_single(base_line + timedelta(minutes=j), j)
                tsf3.append_single(base_line + timedelta(minutes=j), j)

            session.save_changes()

        get = self.store.operations.send(GetTimeSeriesOperation(doc_id, "HeartRate"))
        self.assertEqual(100, len(get.entries))

        # null From, To
        delete_op = TimeSeriesOperation()
        delete_op.name = "Heartrate"
        delete_op.delete(TimeSeriesOperation.DeleteOperation())

        self.store.operations.send(TimeSeriesBatchOperation(doc_id, delete_op))

        get = self.store.operations.send(GetTimeSeriesOperation(doc_id, "HeartRate"))

        self.assertIsNone(get)

        get = self.store.operations.send(GetTimeSeriesOperation(doc_id, "BloodPressure"))
        self.assertEqual(100, len(get.entries))

        # null to

        delete_op = TimeSeriesOperation("BloodPressure")
        delete_op.delete(TimeSeriesOperation.DeleteOperation(base_line + timedelta(minutes=50), None))

        self.store.operations.send(TimeSeriesBatchOperation(doc_id, delete_op))
        get = self.store.operations.send(GetTimeSeriesOperation(doc_id, "BloodPressure"))
        self.assertEqual(50, len(get.entries))

        get = self.store.operations.send(GetTimeSeriesOperation(doc_id, "BodyTemperature"))
        self.assertEqual(100, len(get.entries))

        # null From
        delete_op = TimeSeriesOperation("BodyTemperature")
        delete_op.delete(TimeSeriesOperation.DeleteOperation(None, base_line + timedelta(minutes=19)))

        self.store.operations.send(TimeSeriesBatchOperation(doc_id, delete_op))

        get = self.store.operations.send(GetTimeSeriesOperation(doc_id, "BodyTemperature"))
        self.assertEqual(80, len(get.entries))

    def test_can_delete_timestamps_using_store_operations(self):
        base_line = datetime(2023, 8, 20, 21, 30)
        document_id = "users/ayende"

        with self.store.open_session() as session:
            session.store(User(), document_id)
            session.save_changes()

        time_series_op = TimeSeriesOperation("Heartrate")

        time_series_op.append(
            TimeSeriesOperation.AppendOperation(base_line + timedelta(seconds=1), [59], "watches/fitbit")
        )
        time_series_op.append(
            TimeSeriesOperation.AppendOperation(base_line + timedelta(seconds=2), [61], "watches/fitbit")
        )
        time_series_op.append(
            TimeSeriesOperation.AppendOperation(base_line + timedelta(seconds=3), [60], "watches/fitbit")
        )
        time_series_op.append(
            TimeSeriesOperation.AppendOperation(base_line + timedelta(seconds=4), [62.5], "watches/fitbit")
        )
        time_series_op.append(
            TimeSeriesOperation.AppendOperation(base_line + timedelta(seconds=5), [62], "watches/fitbit")
        )

        time_series_batch = TimeSeriesBatchOperation(document_id, time_series_op)

        self.store.operations.send(time_series_batch)

        time_series_range_result = self.store.operations.send(GetTimeSeriesOperation(document_id, "Heartrate"))

        self.assertEqual(5, len(time_series_range_result.entries))

        time_series_op = TimeSeriesOperation("Heartrate")
        time_series_op.delete(
            TimeSeriesOperation.DeleteOperation(base_line + timedelta(seconds=2), base_line + timedelta(seconds=3))
        )

        time_series_batch = TimeSeriesBatchOperation(document_id, time_series_op)

        self.store.operations.send(time_series_batch)

        time_series_range_result = self.store.operations.send(GetTimeSeriesOperation(document_id, "Heartrate"))

        self.assertEqual(3, len(time_series_range_result.entries))

        value = time_series_range_result.entries[0]
        self.assertEqual(59, value.values[0])
        self.assertEqual(base_line + timedelta(seconds=1), value.timestamp)

        value = time_series_range_result.entries[1]
        self.assertEqual(62.5, value.values[0])
        self.assertEqual(base_line + timedelta(seconds=4), value.timestamp)

        value = time_series_range_result.entries[2]
        self.assertEqual(62, value.values[0])
        self.assertEqual(base_line + timedelta(seconds=5), value.timestamp)

        with self.store.open_session() as session:
            session.delete(document_id)
            session.save_changes()

        with self.store.open_session() as session:
            user = User(name="Oren")
            session.store(user, document_id)
            session.save_changes()

            tsf = session.time_series_for(document_id, "Heartrate")
            tsf.append(base_line + timedelta(minutes=1), [59], "watches/fitbit")
            tsf.append(base_line + timedelta(minutes=2), [69], "watches/fitbit")
            tsf.append(base_line + timedelta(minutes=3), [79], "watches/fitbit")

            session.save_changes()

        with self.store.open_session() as session:
            user = User(name="Oren")
            session.store(user, document_id)
            session.time_series_for(document_id, "Heartrate").delete_at(base_line + timedelta(minutes=2))
            session.save_changes()

        with self.store.open_session() as session:
            vals = session.time_series_for(document_id, "Heartrate").get()

            self.assertEqual(2, len(vals))

            self.assertEqual([59], vals[0].values)
            self.assertEqual("watches/fitbit", vals[0].tag)
            self.assertEqual(base_line + timedelta(minutes=1), vals[0].timestamp)

            self.assertEqual([79], vals[1].values)
            self.assertEqual("watches/fitbit", vals[1].tag)
            self.assertEqual(base_line + timedelta(minutes=3), vals[1].timestamp)

    def test_get_time_series_statistics(self):
        base_line = datetime(2023, 8, 20, 21, 30)
        document_id = "users/ayende"

        with self.store.open_session() as session:
            user = User()
            session.store(user, document_id)

            ts = session.time_series_for(document_id, "heartrate")
            for i in range(11):
                ts.append_single(base_line + timedelta(minutes=i * 10), 72, "watches/fitbit")

            ts = session.time_series_for(document_id, "pressure")
            for i in range(10, 21):
                ts.append_single(base_line + timedelta(minutes=i * 10), 72, "watches/fitbit")

            session.save_changes()

        op = self.store.operations.send(GetTimeSeriesStatisticsOperation(document_id))
        self.assertEqual(document_id, op.document_id)
        self.assertEqual(2, len(op.time_series))

        ts1 = op.time_series[0]
        ts2 = op.time_series[1]

        self.assertEqual("heartrate", ts1.name)
        self.assertEqual("pressure", ts2.name)

        self.assertEqual(11, ts1.number_of_entries)
        self.assertEqual(11, ts2.number_of_entries)

        self.assertEqual(base_line, ts1.start_date)
        self.assertEqual(base_line + timedelta(minutes=10 * 10), ts1.end_date)

        self.assertEqual(base_line + timedelta(minutes=10 * 10), ts2.start_date)
        self.assertEqual(base_line + timedelta(minutes=20 * 10), ts2.end_date)

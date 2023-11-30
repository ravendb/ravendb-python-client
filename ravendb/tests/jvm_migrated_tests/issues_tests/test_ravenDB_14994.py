from datetime import datetime, timedelta

from ravendb.documents.operations.time_series import GetTimeSeriesOperation
from ravendb.infrastructure.entities import User
from ravendb.tests.test_base import TestBase


class TestRavenDB14994(TestBase):
    def setUp(self):
        super().setUp()

    def test_get_on_non_existing_time_series_should_return_null(self):
        document_id = "users/gracjan"
        with self.store.open_session() as session:
            session.store(User(), document_id)
            session.save_changes()

        get = self.store.operations.send(GetTimeSeriesOperation(document_id, "HeartRate"))
        self.assertIsNone(get)

        with self.store.open_session() as session:
            self.assertIsNone(session.time_series_for(document_id, "HeartRate").get())

    def test_get_on_empty_range_should_return_empty_array(self):
        document_id = "users/gracjan"
        base_line = datetime(2023, 8, 20, 21, 30)
        with self.store.open_session() as session:
            session.store(User(), document_id)

            tsf = session.time_series_for(document_id, "HeartRate")
            for i in range(10):
                tsf.append_single(base_line + timedelta(minutes=i), i)

            session.save_changes()

        get = self.store.operations.send(
            GetTimeSeriesOperation(
                document_id, "HeartRate", base_line - timedelta(minutes=2), base_line - timedelta(minutes=1)
            )
        )
        self.assertEqual(0, len(get.entries))

        with self.store.open_session() as session:
            result = session.time_series_for(document_id, "HeartRate").get(
                base_line - timedelta(days=62), base_line - timedelta(days=31)
            )
            self.assertEqual(0, len(result))

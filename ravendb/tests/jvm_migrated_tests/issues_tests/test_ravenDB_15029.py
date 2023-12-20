from datetime import datetime, timedelta

from ravendb.documents.queries.time_series import TimeSeriesRawResult
from ravendb.infrastructure.entities import User
from ravendb.tests.test_base import TestBase

document_id = "users/ayende"
base_line = datetime(2023, 8, 20, 21, 00)
base_line2 = base_line - timedelta(days=1)
ts_name_1 = "Heartrate"
ts_name_2 = "BloodPressure"
tag1 = "watches/fitbit"
tag2 = "watches/apple"


class TestRavenDB15029(TestBase):
    def setUp(self):
        super(TestRavenDB15029, self).setUp()

    def test_session_raw_query_should_not_track_time_series_results_as_document(self):
        with self.store.open_session() as session:
            session.store(User(name="Karmel"), "users/karmel")
            session.time_series_for("users/karmel", ts_name_1).append_single(base_line, 60, tag1)
            session.save_changes()

        with self.store.open_session() as session:
            u = session.load("users/karmel", User)
            query = session.advanced.raw_query(
                "declare timeseries out()\n"
                "{\n"
                "    from HeartRate\n"
                "}\n"
                "from Users as u\n"
                "where name = 'Karmel'\n"
                "select out()",
                TimeSeriesRawResult,
            )

            result = query.first()

            self.assertEqual(1, result.count)
            self.assertEqual(60.0, result.results[0].value)
            self.assertEqual(base_line, result.results[0].timestamp)
            self.assertEqual(tag1, result.results[0].tag)

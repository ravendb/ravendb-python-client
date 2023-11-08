from datetime import datetime, timedelta

from ravendb import InMemoryDocumentSessionOperations
from ravendb.tests.test_base import TestBase, User


class TestTimeSeriesRangesCache(TestBase):
    def setUp(self):
        super().setUp()

    def test_should_get_partial_range_from_cache(self):
        base_line = datetime(2023, 8, 20, 21, 30)
        doc_id = "users/ayende"
        ts_name = "Heartrate"

        with self.store.open_session() as session:
            user = User(name="Oren")
            session.store(user, doc_id)
            session.time_series_for(doc_id, ts_name).append(base_line + timedelta(minutes=1), [59], "watches/fitbit")
            session.save_changes()

        with self.store.open_session() as session:
            val = session.time_series_for(doc_id, ts_name).get()[0]

            self.assertEqual([59], val.values)
            self.assertEqual("watches/fitbit", val.tag)
            self.assertEqual(base_line + timedelta(minutes=1), val.timestamp)

            self.assertEqual(1, session.advanced.number_of_requests)

            # should load from cache
            val = session.time_series_for(doc_id, ts_name).get(base_line, base_line + timedelta(days=1))[0]

            self.assertEqual([59], val.values)
            self.assertEqual("watches/fitbit", val.tag)
            self.assertEqual(base_line + timedelta(minutes=1), val.timestamp)

            self.assertEqual(1, session.advanced.number_of_requests)

            in_memory_session: InMemoryDocumentSessionOperations = session

            cache = in_memory_session.time_series_by_doc_id[doc_id]
            self.assertIsNotNone(cache)

            ranges = cache.get(ts_name)
            self.assertIsNotNone(ranges)
            self.assertEqual(1, len(ranges))

    def test_should_get_time_series_value_from_cache(self):
        base_line = datetime(2023, 8, 20, 21, 30)
        doc_id = "users/ayende"
        ts_name = "Heartrate"

        with self.store.open_session() as session:
            user = User(name="Oren")
            session.store(user, doc_id)
            session.time_series_for(doc_id, ts_name).append(base_line + timedelta(minutes=1), [59], "watches/fitbit")
            session.save_changes()

        with self.store.open_session() as session:
            val = session.time_series_for("users/ayende", ts_name).get()[0]

            self.assertEqual([59], val.values)
            self.assertEqual("watches/fitbit", val.tag)
            self.assertEqual(base_line + timedelta(minutes=1), val.timestamp)

            self.assertEqual(1, session.advanced.number_of_requests)

            # should load from cache
            val = session.time_series_for(doc_id, ts_name).get()[0]
            self.assertEqual([59], val.values)
            self.assertEqual("watches/fitbit", val.tag)
            self.assertEqual(base_line + timedelta(minutes=1), val.timestamp)

            self.assertEqual(1, session.advanced.number_of_requests)

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

    def test_merge_time_series_range_in_cache(self):
        base_line = datetime(2023, 8, 20, 21, 30)
        doc_id = "users/ayende"
        ts_name = "Heartrate"
        tag = "watches/fitbit"

        with self.store.open_session() as session:
            session.store(User(name="Oren"), doc_id)
            session.save_changes()

        with self.store.open_session() as session:
            tsf = session.time_series_for(doc_id, ts_name)
            for i in range(360):
                tsf.append_single(base_line + timedelta(seconds=i * 10), 6, tag)
            session.save_changes()

        with self.store.open_session() as session:
            vals = session.time_series_for(doc_id, ts_name).get(
                base_line + timedelta(minutes=2), base_line + timedelta(minutes=10)
            )
            self.assertEqual(1, session.advanced.number_of_requests)
            self.assertEqual(49, len(vals))
            self.assertEqual(base_line + timedelta(minutes=2), vals[0].timestamp)
            self.assertEqual(base_line + timedelta(minutes=10), vals[48].timestamp)

            # should load partial range from cache
            vals = session.time_series_for(doc_id, ts_name).get(
                base_line + timedelta(minutes=5), base_line + timedelta(minutes=7)
            )

            self.assertEqual(1, session.advanced.number_of_requests)
            self.assertEqual(13, len(vals))
            self.assertEqual(base_line + timedelta(minutes=5), vals[0].timestamp)
            self.assertEqual(base_line + timedelta(minutes=7), vals[12].timestamp)

            # should go to server
            vals = session.time_series_for(doc_id, ts_name).get(
                base_line + timedelta(minutes=40), base_line + timedelta(minutes=50)
            )

            self.assertEqual(2, session.advanced.number_of_requests)
            self.assertEqual(61, len(vals))
            self.assertEqual(base_line + timedelta(minutes=40), vals[0].timestamp)
            self.assertEqual(base_line + timedelta(minutes=50), vals[60].timestamp)

            cache = session.time_series_by_doc_id.get(doc_id)
            self.assertIsNotNone(cache)
            ranges = cache.get(ts_name)
            self.assertIsNotNone(ranges)
            self.assertEqual(2, len(ranges))

            self.assertEqual(base_line + timedelta(minutes=2), ranges[0].from_date)
            self.assertEqual(base_line + timedelta(minutes=10), ranges[0].to_date)

            self.assertEqual(base_line + timedelta(minutes=40), ranges[1].from_date)
            self.assertEqual(base_line + timedelta(minutes=50), ranges[1].to_date)

            # should go to server to get [0, 2] and merge it into existing [2, 10]
            vals = session.time_series_for(doc_id, ts_name).get(base_line, base_line + timedelta(minutes=5))

            self.assertEqual(3, session.advanced.number_of_requests)

            self.assertEqual(31, len(vals))
            self.assertEqual(base_line, vals[0].timestamp)
            self.assertEqual(base_line + timedelta(minutes=5), vals[30].timestamp)

            self.assertEqual(2, len(ranges))

            self.assertEqual(base_line + timedelta(minutes=0), ranges[0].from_date)
            self.assertEqual(base_line + timedelta(minutes=10), ranges[0].to_date)

            self.assertEqual(base_line + timedelta(minutes=40), ranges[1].from_date)
            self.assertEqual(base_line + timedelta(minutes=50), ranges[1].to_date)

            # should go to server to get [10, 16] and merge it into existing [0, 10]
            vals = session.time_series_for(doc_id, ts_name).get(
                base_line + timedelta(minutes=8), base_line + timedelta(minutes=16)
            )

            self.assertEqual(4, session.advanced.number_of_requests)

            self.assertEqual(49, len(vals))
            self.assertEqual(base_line + timedelta(minutes=8), vals[0].timestamp)
            self.assertEqual(base_line + timedelta(minutes=16), vals[48].timestamp)

            self.assertEqual(2, len(ranges))

            self.assertEqual(base_line + timedelta(minutes=0), ranges[0].from_date)
            self.assertEqual(base_line + timedelta(minutes=16), ranges[0].to_date)
            self.assertEqual(base_line + timedelta(minutes=40), ranges[1].from_date)
            self.assertEqual(base_line + timedelta(minutes=50), ranges[1].to_date)

            # should go to server to get range [17, 19]
            # and add it between [10, 16] and [40, 50]

            vals = session.time_series_for(doc_id, ts_name).get(
                base_line + timedelta(minutes=17), base_line + timedelta(minutes=19)
            )

            self.assertEqual(5, session.advanced.number_of_requests)

            self.assertEqual(13, len(vals))
            self.assertEqual(base_line + timedelta(minutes=17), vals[0].timestamp)
            self.assertEqual(base_line + timedelta(minutes=19), vals[12].timestamp)

            self.assertEqual(base_line + timedelta(minutes=0), ranges[0].from_date)
            self.assertEqual(base_line + timedelta(minutes=16), ranges[0].to_date)
            self.assertEqual(base_line + timedelta(minutes=17), ranges[1].from_date)
            self.assertEqual(base_line + timedelta(minutes=19), ranges[1].to_date)
            self.assertEqual(base_line + timedelta(minutes=40), ranges[2].from_date)
            self.assertEqual(base_line + timedelta(minutes=50), ranges[2].to_date)

            # should go to server to get range [19, 40]
            # and merge the result with existing ranges [17, 19] and [40, 50]
            # into single range [17, 50]

            vals = session.time_series_for(doc_id, ts_name).get(
                base_line + timedelta(minutes=18), base_line + timedelta(minutes=48)
            )
            self.assertEqual(6, session.advanced.number_of_requests)

            self.assertEqual(181, len(vals))
            self.assertEqual(base_line + timedelta(minutes=18), vals[0].timestamp)
            self.assertEqual(base_line + timedelta(minutes=48), vals[180].timestamp)

            self.assertEqual(2, len(ranges))

            self.assertEqual(base_line + timedelta(minutes=0), ranges[0].from_date)
            self.assertEqual(base_line + timedelta(minutes=16), ranges[0].to_date)
            self.assertEqual(base_line + timedelta(minutes=17), ranges[1].from_date)
            self.assertEqual(base_line + timedelta(minutes=50), ranges[1].to_date)

            # should get to server to get range [16, 17]
            # and merge the result with existing ranges [0, 16] and [17, 50]
            # into single range [0, 50]

            vals = session.time_series_for(doc_id, ts_name).get(
                base_line + timedelta(minutes=12), base_line + timedelta(minutes=22)
            )

            self.assertEqual(7, session.advanced.number_of_requests)

            self.assertEqual(61, len(vals))
            self.assertEqual(base_line + timedelta(minutes=12), vals[0].timestamp)
            self.assertEqual(base_line + timedelta(minutes=22), vals[60].timestamp)

            self.assertEqual(1, len(ranges))

            self.assertEqual(base_line + timedelta(minutes=0), ranges[0].from_date)
            self.assertEqual(base_line + timedelta(minutes=50), ranges[0].to_date)

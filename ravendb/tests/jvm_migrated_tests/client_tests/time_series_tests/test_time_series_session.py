from datetime import datetime, timedelta

from ravendb import InMemoryDocumentSessionOperations
from ravendb.infrastructure.entities import User
from ravendb.tests.test_base import TestBase


class TestTimeSeriesSession(TestBase):
    def setUp(self):
        super(TestTimeSeriesSession, self).setUp()

    def test_can_create_simple_time_series(self):
        today = datetime.today()
        base_line = datetime(today.year, today.month, today.day)

        with self.store.open_session() as session:
            user = User(name="Oren")
            session.store(user, "users/ayende")
            timestamp = base_line + timedelta(minutes=1)
            session.time_series_for("users/ayende", "Heartrate").append_single(timestamp, 59, "watches/fitbit")
            session.save_changes()

        with self.store.open_session() as session:
            val = session.time_series_for("users/ayende", "Heartrate").get()[0]
            self.assertEqual([59], val.values)
            self.assertEqual("watches/fitbit", val.tag)
            self.assertEqual(timestamp, val.timestamp)

    def test_can_crate_simple_time_series_2(self):
        today = datetime.today()
        base_line = datetime(today.year, today.month, today.day)

        with self.store.open_session() as session:
            user = User(name="Oren")
            session.store(user, "users/ayende")

            tsf = session.time_series_for("users/ayende", "Heartrate")
            tsf.append_single(base_line + timedelta(1), 59, "watches/fitbit")
            tsf.append_single(base_line + timedelta(2), 60, "watches/fitbit")
            tsf.append_single(base_line + timedelta(3), 61, "watches/fitbit")

            session.save_changes()

        with self.store.open_session() as session:
            val = session.time_series_for("users/ayende", "Heartrate").get()
            self.assertEqual(3, len(val))

    def test_can_get_time_series_names(self):
        base_line = datetime(2023, 8, 20, 21, 30)

        with self.store.open_session() as session:
            user = User()
            session.store(user, "users/karmel")
            session.time_series_for("users/karmel", "Nasdaq2").append_single(datetime.now(), 7547.31, "web")
            session.save_changes()

        with self.store.open_session() as session:
            session.time_series_for("users/karmel", "Heartrate2").append_single(datetime.now(), 7547.31, "web")
            session.save_changes()

        with self.store.open_session() as session:
            session.store(User(), "users/ayende")
            session.time_series_for("users/ayende", "Nasdaq").append_single(datetime.now(), 7547.31, "web")
            session.save_changes()

        with self.store.open_session() as session:
            session.time_series_for("users/ayende", "Heartrate").append_single(
                datetime.now() + timedelta(minutes=1), 58, "fitbit"
            )
            session.save_changes()

        with self.store.open_session() as session:
            user = session.load("users/ayende", User)
            ts_names = session.advanced.get_time_series_for(user)
            self.assertEqual(2, len(ts_names))

            # should be sorted
            self.assertEqual("Heartrate", ts_names[0])
            self.assertEqual("Nasdaq", ts_names[1])

        with self.store.open_session() as session:
            session.time_series_for("users/ayende", "heartrate").append_single(
                base_line + timedelta(minutes=1), 58, "fitbit"
            )
            session.save_changes()

        with self.store.open_session() as session:
            user = session.load("users/ayende", User)
            ts_names = session.advanced.get_time_series_for(user)
            self.assertEqual(2, len(ts_names))

            # should preserve original casing
            self.assertEqual("Heartrate", ts_names[0])
            self.assertEqual("Nasdaq", ts_names[1])

    def test_can_get_time_series_names_2(self):
        base_line = datetime(2023, 8, 20, 21, 30)

        with self.store.open_session() as session:
            user = User()
            user.name = "Oren"
            session.store(user, "users/ayende")
            session.save_changes()

        offset = 0

        for i in range(100):
            with self.store.open_session() as session:
                tsf = session.time_series_for("users/ayende", "Heartrate")

                for j in range(1000):
                    tsf.append_single(base_line + timedelta(minutes=offset), offset, "watches/fitbit")
                    offset += 1

                session.save_changes()

        offset = 0

        for i in range(100):
            with self.store.open_session() as session:
                tsf = session.time_series_for("users/ayende", "Pulse")

                for j in range(1000):
                    tsf.append_single(base_line + timedelta(minutes=offset), offset, "watches/fitbit")
                    offset += 1

                session.save_changes()

        offset = 0

        for i in range(100):
            with self.store.open_session() as session:
                tsf = session.time_series_for("users/ayende", "Pulse")

                for j in range(1000):
                    tsf.append_single(base_line + timedelta(minutes=offset), offset, "watches/fitbit")
                    offset += 1

                session.save_changes()

        with self.store.open_session() as session:
            vals = session.time_series_for("users/ayende", "Heartrate").get(None, None)
            self.assertEqual(100000, len(vals))

            for i in range(100000):
                self.assertEqual(base_line + timedelta(minutes=i), vals[i].timestamp)
                self.assertEqual(i, vals[i].values[0])

        with self.store.open_session() as session:
            user = session.load("users/ayende", User)
            ts_names = session.advanced.get_time_series_for(user)

            # should be sorted
            self.assertEqual("Heartrate", ts_names[0])
            self.assertEqual("Pulse", ts_names[1])

    def test_using_different_tags(self):
        base_line = datetime(2023, 8, 20, 21, 30)

        with self.store.open_session() as session:
            user = User()
            user.name = "Oren"
            session.store(user, "users/ayende")

            tsf = session.time_series_for("users/ayende", "Heartrate")
            tsf.append_single(base_line + timedelta(minutes=1), 59, "watches/fitbit")
            tsf.append_single(base_line + timedelta(minutes=2), 70, "watches/apple")

            session.save_changes()

        with self.store.open_session() as session:
            vals = session.time_series_for("users/ayende", "Heartrate").get(None, None)

            self.assertEqual(2, len(vals))
            self.assertEqual([59], vals[0].values)
            self.assertEqual("watches/fitbit", vals[0].tag)
            self.assertEqual(base_line + timedelta(minutes=1), vals[0].timestamp)

            self.assertEqual([70], vals[1].values)
            self.assertEqual("watches/apple", vals[1].tag)
            self.assertEqual(base_line + timedelta(minutes=2), vals[1].timestamp)

    def test_can_store_and_read_multiple_timestamps(self):
        base_line = datetime(2023, 8, 20, 21, 30)
        with self.store.open_session() as session:
            user = User()
            user.name = "Oren"
            session.store(user, "users/ayende")

            tsf = session.time_series_for("users/ayende", "Heartrate")
            tsf.append_single(base_line + timedelta(minutes=1), 59, "watches/fitbit")

            session.save_changes()

        with self.store.open_session() as session:
            tsf = session.time_series_for("users/ayende", "Heartrate")
            tsf.append_single(base_line + timedelta(minutes=2), 61, "watches/fitbit")
            tsf.append_single(base_line + timedelta(minutes=3), 62, "watches/apple-watch")

            session.save_changes()

        with self.store.open_session() as session:
            vals = session.time_series_for("users/ayende", "Heartrate").get(None, None)

            self.assertEqual(3, len(vals))

            self.assertEqual([59], vals[0].values)
            self.assertEqual("watches/fitbit", vals[0].tag)
            self.assertEqual(base_line + timedelta(minutes=1), vals[0].timestamp)

            self.assertEqual([61], vals[1].values)
            self.assertEqual("watches/fitbit", vals[1].tag)
            self.assertEqual(base_line + timedelta(minutes=2), vals[1].timestamp)

            self.assertEqual([62], vals[2].values)
            self.assertEqual("watches/apple-watch", vals[2].tag)
            self.assertEqual(base_line + timedelta(minutes=3), vals[2].timestamp)

    def test_can_request_non_existing_time_series_range(self):
        base_line = datetime(2023, 8, 20, 21, 30)

        with self.store.open_session() as session:
            user = User()
            user.name = "Oren"
            session.store(user, "users/ayende")
            tsf = session.time_series_for("users/ayende", "Heartrate")
            tsf.append_single(base_line, 58, "watches/fitbit")
            tsf.append_single(base_line + timedelta(minutes=10), 60, "watches/fitbit")
            session.save_changes()

        with self.store.open_session() as session:
            vals = session.time_series_for("users/ayende", "Heartrate").get(
                base_line - timedelta(minutes=10), base_line - timedelta(minutes=5)
            )
            self.assertEqual(0, len(vals))

            vals = session.time_series_for("users/ayende", "Heartrate").get(
                base_line + timedelta(minutes=5), base_line + timedelta(minutes=9)
            )
            self.assertEqual(0, len(vals))

    def test_should_delete_time_series_upon_document_deletion(self):
        base_line = datetime(2023, 8, 20, 21, 30)
        user_id = "users/ayende"

        with self.store.open_session() as session:
            user = User()
            user.name = "Oren"
            session.store(user, user_id)

            time_series_for = session.time_series_for(user_id, "Heartrate")
            time_series_for.append_single(base_line + timedelta(minutes=1), 59, "watches/fitbit")
            time_series_for.append_single(base_line + timedelta(minutes=2), 59, "watches/fitbit")

            session.time_series_for(user_id, "Heartrate2").append_single(
                (base_line + timedelta(minutes=1)), 59, "watches/apple"
            )
            session.save_changes()

        with self.store.open_session() as session:
            session.delete(user_id)
            session.save_changes()

        with self.store.open_session() as session:
            vals = session.time_series_for(user_id, "Heartrate").get(None, None)
            self.assertIsNone(vals)

            vals = session.time_series_for(user_id, "Heartrate2").get(None, None)
            self.assertIsNone(vals)

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

        with self.store.open_session() as session:
            tsf = session.time_series_for(doc_id, "Heartrate")

            entries = tsf.get()
            self.assertEqual(100, len(entries))

            # null From, To
            tsf.delete()
            session.save_changes()

        with self.store.open_session() as session:
            entries = session.time_series_for(doc_id, "Heartrate").get()
            self.assertIsNone(entries)

        with self.store.open_session() as session:
            tsf = session.time_series_for(doc_id, "BloodPressure")

            entries = tsf.get()
            self.assertEqual(100, len(entries))

            # null to
            tsf.delete(base_line + timedelta(minutes=50), None)
            session.save_changes()

        with self.store.open_session() as session:
            entries = session.time_series_for(doc_id, "BloodPressure").get()
            self.assertEqual(50, len(entries))

        with self.store.open_session() as session:
            tsf = session.time_series_for(doc_id, "BodyTemperature")

            entries = tsf.get()
            self.assertEqual(100, len(entries))

            # null from
            tsf.delete(None, base_line + timedelta(minutes=19))
            session.save_changes()

        with self.store.open_session() as session:
            entries = session.time_series_for(doc_id, "BodyTemperature").get()
            self.assertEqual(80, len(entries))

    def test_using_different_number_of_values_small_to_large(self):
        base_line = datetime(2023, 8, 20, 21, 30)
        with self.store.open_session() as session:
            user = User("Oren")
            session.store(user, "users/ayende")

            tsf = session.time_series_for("users/ayende", "Heartrate")
            tsf.append_single(base_line + timedelta(minutes=1), 59, "watches/fitbit")
            tsf.append(base_line + timedelta(minutes=2), [70, 120, 80], "watches/apple")
            tsf.append_single(base_line + timedelta(minutes=3), 69, "watches/fitbit")

            session.save_changes()

        with self.store.open_session() as session:
            vals = session.time_series_for("users/ayende", "Heartrate").get(None, None)
            self.assertEqual(3, len(vals))

            self.assertEqual([59], vals[0].values)
            self.assertEqual("watches/fitbit", vals[0].tag)
            self.assertEqual(base_line + timedelta(minutes=1), vals[0].timestamp)

            self.assertEqual([70, 120, 80], vals[1].values)
            self.assertEqual("watches/apple", vals[1].tag)
            self.assertEqual(base_line + timedelta(minutes=2), vals[1].timestamp)

            self.assertEqual([69], vals[2].values)
            self.assertEqual("watches/fitbit", vals[2].tag)
            self.assertEqual(base_line + timedelta(minutes=3), vals[2].timestamp)

    def test_time_series_should_be_case_insensitive_and_keep_original_casing(self):
        base_line = datetime(2023, 8, 20, 21, 30)
        with self.store.open_session() as session:
            user = User("Oren")
            session.store(user, "users/ayende")

            session.time_series_for("users/ayende", "Heartrate").append_single(
                base_line + timedelta(minutes=1), 59, "watches/fitbit"
            )
            session.save_changes()

        with self.store.open_session() as session:
            session.time_series_for("users/ayende", "HeartRate").append_single(
                base_line + timedelta(minutes=2), 60, "watches/fitbit"
            )
            session.save_changes()

        with self.store.open_session() as session:
            user = session.load("users/ayende", User)
            val = session.time_series_for("users/ayende", "heartrate").get()

            self.assertEqual([59], val[0].values)
            self.assertEqual("watches/fitbit", val[0].tag)
            self.assertEqual(base_line + timedelta(minutes=1), val[0].timestamp)

            self.assertEqual([60], val[1].values)
            self.assertEqual("watches/fitbit", val[1].tag)
            self.assertEqual(base_line + timedelta(minutes=2), val[1].timestamp)

            self.assertEqual(["Heartrate"], session.advanced.get_time_series_for(user))

    def test_get_single_time_series_when_no_time_series(self):
        with self.store.open_session() as session:
            user = User()
            session.store(user, "users/karmel")
            session.save_changes()

        with self.store.open_session() as session:
            user = session.load("users/karmel", User)
            ts = session.time_series_for_entity(user, "unicorns").get()
            self.assertIsNone(ts)

    def test_should_evict_time_series_upon_entity_eviction(self):
        base_line = datetime(2023, 8, 20, 21, 30)
        document_id = "users/ayende"

        with self.store.open_session() as session:
            user = User()
            user.name = "Oren"

            session.store(user, document_id)

            tsf = session.time_series_for(document_id, "Heartrate")

            for i in range(60):
                tsf.append_single(base_line + timedelta(minutes=i), 100 + i, "watches/fitbit")

            tsf = session.time_series_for(document_id, "BloodPressure")

            for i in range(10):
                tsf.append(base_line + timedelta(minutes=i), [120 - i, 80 + i], "watches/apple")

            session.save_changes()

        with self.store.open_session() as session:
            user = session.load(document_id, User)

            tsf = session.time_series_for_entity(user, "Heartrate")

            vals = tsf.get(base_line, base_line + timedelta(minutes=10))

            self.assertEqual(11, len(vals))

            vals = tsf.get(base_line + timedelta(minutes=20), base_line + timedelta(minutes=50))

            self.assertEqual(31, len(vals))

            tsf = session.time_series_for_entity(user, "BloodPressure")

            vals = tsf.get()

            self.assertEqual(10, len(vals))

            session_operations: "InMemoryDocumentSessionOperations" = session

            self.assertEqual(1, len(session_operations.time_series_by_doc_id))
            cache = session_operations.time_series_by_doc_id.get(document_id)
            self.assertEqual(2, len(cache))
            ranges = cache.get("Heartrate")
            self.assertEqual(2, len(ranges))

            self.assertEqual(base_line, ranges[0].from_date)
            self.assertEqual(base_line + timedelta(minutes=10), ranges[0].to_date)
            self.assertEqual(11, len(ranges[0].entries))
            self.assertEqual(base_line + timedelta(minutes=20), ranges[1].from_date)
            self.assertEqual(base_line + timedelta(minutes=50), ranges[1].to_date)

            self.assertEqual(31, len(ranges[1].entries))
            ranges = cache.get("BloodPressure")
            self.assertEqual(1, len(ranges))
            self.assertIsNone(ranges[0].from_date)
            self.assertIsNone(ranges[0].to_date)
            self.assertEqual(10, len(ranges[0].entries))

            session.advanced.evict(user)

            cache = session_operations.time_series_by_doc_id.get(document_id, None)
            self.assertIsNone(cache)
            self.assertEqual(0, len(session_operations.time_series_by_doc_id))

    def test_using_different_number_of_values_large_to_small(self):
        base_line = datetime(2023, 8, 20, 21, 30)
        document_id = "users/ayende"

        with self.store.open_session() as session:
            user = User(name="Oren")
            session.store(user, document_id)

            tsf = session.time_series_for(document_id, "Heartrate")
            tsf.append(base_line + timedelta(minutes=1), [70, 120, 80], "watches/apple")
            tsf.append_single(base_line + timedelta(minutes=2), 59, "watches/fitbit")
            tsf.append_single(base_line + timedelta(minutes=3), 69, "watches/fitbit")

            session.save_changes()

        with self.store.open_session() as session:
            vals = session.time_series_for(document_id, "Heartrate").get()
            self.assertEqual(3, len(vals))

            self.assertEqual([70, 120, 80], vals[0].values)
            self.assertEqual("watches/apple", vals[0].tag)
            self.assertEqual(base_line + timedelta(minutes=1), vals[0].timestamp)

            self.assertEqual([59], vals[1].values)
            self.assertEqual("watches/fitbit", vals[1].tag)
            self.assertEqual(base_line + timedelta(minutes=2), vals[1].timestamp)

            self.assertEqual([69], vals[2].values)
            self.assertEqual("watches/fitbit", vals[2].tag)
            self.assertEqual(base_line + timedelta(minutes=3), vals[2].timestamp)

    def test_can_store_large_number_of_values(self):
        base_line = datetime(2023, 8, 20, 21, 30)
        document_id = "users/ayende"

        with self.store.open_session() as session:
            user = User("Oren")
            session.store(user, document_id)
            session.save_changes()

        offset = 0

        for i in range(10):
            with self.store.open_session() as session:
                tsf = session.time_series_for(document_id, "Heartrate")
                for j in range(1000):
                    tsf.append(base_line + timedelta(minutes=offset), [offset], "watches/fitbit")
                    offset += 1
                session.save_changes()

        with self.store.open_session() as session:
            vals = session.time_series_for(document_id, "Heartrate").get()
            self.assertEqual(10000, len(vals))

            for i in range(10000):
                self.assertEqual(base_line + timedelta(minutes=i), vals[i].timestamp)
                self.assertEqual(i, vals[i].values[0])

    def test_can_skip_and_take_time_series(self):
        base_line = datetime(2023, 8, 20, 21, 30)
        document_id = "users/ayende"

        with self.store.open_session() as session:
            user = User(name="Oren")
            session.store(user, document_id)

            tsf = session.time_series_for(document_id, "Heartrate")
            for i in range(100):
                tsf.append_single(base_line + timedelta(minutes=i), 100 + i, "watches/fitbit")

            session.save_changes()

        with self.store.open_session() as session:
            vals = session.time_series_for(document_id, "Heartrate").get(start=5, page_size=20)
            self.assertEqual(20, len(vals))

            for i in range(len(vals)):
                self.assertEqual(base_line + timedelta(minutes=5 + i), vals[i].timestamp)
                self.assertEqual(105 + i, vals[i].value)

    def test_can_store_values_out_of_order(self):
        base_line = datetime(2023, 8, 20, 21, 30)
        document_id = "users/ayende"

        with self.store.open_session() as session:
            user = User(name="Oren")
            session.store(user, document_id)
            session.save_changes()

        retries = 1000

        offset = 0

        with self.store.open_session() as session:
            tsf = session.time_series_for(document_id, "Heartrate")
            for i in range(retries):
                tsf.append(base_line + timedelta(minutes=offset), [offset], "watches/fitbit")
                offset += 5

            session.save_changes()

        offset = 1

        with self.store.open_session() as session:
            tsf = session.time_series_for(document_id, "Heartrate")
            vals = tsf.get()
            self.assertEqual(retries, len(vals))

            for i in range(retries):
                tsf.append(base_line + timedelta(minutes=offset), [offset], "watches/fitbit")
                offset += 5

            session.save_changes()

        with self.store.open_session() as session:
            vals = session.time_series_for(document_id, "Heartrate").get()
            self.assertEqual(2 * retries, len(vals))

            offset = 0

            for i in range(0, retries, 2):
                self.assertEqual(base_line + timedelta(minutes=offset), vals[i].timestamp)
                self.assertEqual(offset, vals[i].values[0])

                offset += 1
                i += 1

                self.assertEqual(base_line + timedelta(minutes=offset), vals[i].timestamp)
                self.assertEqual(offset, vals[i].values[0])

                offset += 4

    def test_can_delete_timestamp(self):
        base_line = datetime(2023, 8, 20, 21, 30)
        document_id = "users/ayende"

        with self.store.open_session() as session:
            user = User(name="Oren")
            session.store(user, document_id)

            tsf = session.time_series_for(document_id, "Heartrate")
            tsf.append_single(base_line + timedelta(minutes=1), 59, "watches/fitbit")
            tsf.append_single(base_line + timedelta(minutes=2), 69, "watches/fitbit")
            tsf.append_single(base_line + timedelta(minutes=3), 79, "watches/fitbit")

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

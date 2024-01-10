import time
from datetime import timedelta, datetime
from typing import List

from ravendb.documents.session.time_series import TimeSeriesEntry
from ravendb.infrastructure.entities import User
from ravendb.tests.test_base import TestBase
from ravendb.tools.raven_test_helper import RavenTestHelper


class TestTimeSeriesBulkInsert(TestBase):
    def setUp(self):
        super().setUp()

    def test_should_delete_time_series_upon_document_deletion(self):
        base_line = RavenTestHelper.utc_this_month()
        document_id = "users/ayende"

        with self.store.bulk_insert() as bulk_insert:
            user = User()
            user.name = "Oren"
            bulk_insert.store_as(user, document_id)

            with bulk_insert.time_series_for(document_id, "Heartrate") as time_series_bulk_insert:
                time_series_bulk_insert.append_single(base_line + timedelta(minutes=1), 59, "watches/fitbit")
                time_series_bulk_insert.append_single(base_line + timedelta(minutes=2), 59, "watches/fitbit")

            with bulk_insert.time_series_for(document_id, "Heartrate2") as time_series_bulk_insert_2:
                time_series_bulk_insert_2.append_single(base_line + timedelta(minutes=1), 59, "watches/apple")

        with self.store.open_session() as session:
            session.delete(document_id)
            session.save_changes()

        with self.store.open_session() as session:
            vals = session.time_series_for(document_id, "Heartrate").get(None, None)
            self.assertIsNone(vals)

            vals = session.time_series_for(document_id, "Heartrate2").get(None, None)
            self.assertIsNone(vals)

    def test_can_request_non_existing_time_series_range(self):
        base_line = RavenTestHelper.utc_this_month()
        document_id = "users/ayende"

        with self.store.bulk_insert() as bulk_insert:
            user = User(name="Oren")
            bulk_insert.store_as(user, document_id)

            with bulk_insert.time_series_for(document_id, "Heartrate") as time_series_bulk_insert:
                time_series_bulk_insert.append_single(base_line, 58, "watches/fitbit")
                time_series_bulk_insert.append_single(base_line + timedelta(minutes=10), 60, "watches/fitbit")

        with self.store.open_session() as session:
            vals = session.time_series_for("users/ayende", "Heartrate").get(
                base_line - timedelta(minutes=10), base_line - timedelta(minutes=5)
            )

            self.assertEqual(0, len(vals))

            vals = session.time_series_for("users/ayende", "Heartrate").get(
                base_line + timedelta(minutes=5), base_line + timedelta(minutes=9)
            )

            self.assertEqual(0, len(vals))

    def test_can_store_and_read_multiple_timestamps(self):
        base_line = RavenTestHelper.utc_today()
        document_id = "users/ayende"

        with self.store.bulk_insert() as bulk_insert:
            user = User(name="Oren")
            bulk_insert.store_as(user, document_id)

            with bulk_insert.time_series_for(document_id, "Heartrate") as time_series_bulk_insert:
                time_series_bulk_insert.append_single(base_line + timedelta(minutes=1), 59, "watches/fitbit")

        with self.store.bulk_insert() as bulk_insert:
            with bulk_insert.time_series_for(document_id, "Heartrate") as time_series_bulk_insert:
                time_series_bulk_insert.append_single(base_line + timedelta(minutes=2), 61, "watches/fitbit")
                time_series_bulk_insert.append_single(base_line + timedelta(minutes=3), 62, "watches/apple-watch")

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

    def test_using_different_tags(self):
        base_line = RavenTestHelper.utc_this_month()
        document_id = "users/ayende"

        with self.store.bulk_insert() as bulk_insert:
            user = User(name="Oren")
            bulk_insert.store_as(user, document_id)

            with bulk_insert.time_series_for(document_id, "Heartrate") as time_series_bulk_insert:
                time_series_bulk_insert.append_single(base_line + timedelta(minutes=1), 59, "watches/fitbit")
                time_series_bulk_insert.append_single(base_line + timedelta(minutes=2), 70, "watches/apple")

        with self.store.open_session() as session:
            vals = session.time_series_for("users/ayende", "Heartrate").get(None, None)

            self.assertEqual(2, len(vals))

            self.assertEqual([59], vals[0].values)
            self.assertEqual("watches/fitbit", vals[0].tag)
            self.assertEqual(base_line + timedelta(minutes=1), vals[0].timestamp)

            self.assertEqual([70], vals[1].values)
            self.assertEqual("watches/apple", vals[1].tag)
            self.assertEqual(base_line + timedelta(minutes=2), vals[1].timestamp)

    def test_can_create_simple_time_series(self):
        base_line = RavenTestHelper.utc_this_month()
        document_id = "users/ayende"

        with self.store.bulk_insert() as bulk_insert:
            user = User(name="Oren")
            bulk_insert.store_as(user, document_id)

            with bulk_insert.time_series_for(document_id, "Heartrate") as time_series_bulk_insert:
                time_series_bulk_insert.append_single(base_line + timedelta(minutes=1), 59, "watches/fitbit")

        with self.store.open_session() as session:
            val = session.time_series_for(document_id, "Heartrate").get()[0]

            self.assertEqual([59], val.values)
            self.assertEqual("watches/fitbit", val.tag)
            self.assertEqual(base_line + timedelta(minutes=1), val.timestamp)

    def test_can_get_time_series_names_2(self):
        base_line = RavenTestHelper.utc_this_month()
        document_id = "users/ayende"

        with self.store.open_session() as session:
            user = User(name="Oren")
            session.store(user, "users/ayende")
            session.save_changes()

        offset = 0

        for i in range(100):
            with self.store.bulk_insert() as bulk_insert:
                with bulk_insert.time_series_for(document_id, "Heartrate") as time_series_bulk_insert:
                    for j in range(1000):
                        time_series_bulk_insert.append_single(
                            base_line + timedelta(minutes=offset), offset + 1, "watches/fitbit"
                        )
                        offset += 1

        offset = 0

        for i in range(100):
            with self.store.bulk_insert() as bulk_insert:
                with bulk_insert.time_series_for(document_id, "Pulse") as time_series_bulk_insert:
                    for j in range(1000):
                        time_series_bulk_insert.append_single(
                            base_line + timedelta(minutes=offset), offset + 1, "watches/fitbit"
                        )
                        offset += 1

        with self.store.open_session() as session:
            vals = session.time_series_for("users/ayende", "Heartrate").get(None, None)

            self.assertEqual(100000, len(vals))

            for i in range(100000):
                self.assertEqual(base_line + timedelta(minutes=i), vals[i].timestamp)
                self.assertEqual(1 + i, vals[i].values[0])

        with self.store.open_session() as session:
            vals = session.time_series_for("users/ayende", "Pulse").get(None, None)
            self.assertEqual(100000, len(vals))

            for i in range(100000):
                self.assertEqual(base_line + timedelta(minutes=i), vals[i].timestamp)
                self.assertEqual(1 + i, vals[i].value)

        with self.store.open_session() as session:
            user = session.load("users/ayende", User)
            ts_names = session.advanced.get_time_series_for(user)

            # should be sorted
            self.assertEqual("Heartrate", ts_names[0])
            self.assertEqual("Pulse", ts_names[1])

    def test_using_different_number_of_values_small_to_large(self):
        base_line = RavenTestHelper.utc_this_month()
        document_id = "users/ayende"

        with self.store.bulk_insert() as bulk_insert:
            user = User(name="Oren")
            bulk_insert.store_as(user, document_id)

            with bulk_insert.time_series_for(document_id, "Heartrate") as time_series_bulk_insert:
                time_series_bulk_insert.append_single(base_line + timedelta(minutes=1), 59, "watches/fitbit")
                time_series_bulk_insert.append(base_line + timedelta(minutes=2), [70, 120, 80], "watches/apple")
                time_series_bulk_insert.append_single(base_line + timedelta(minutes=3), 69, "watches/fitbit")

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

    def test_error_handling(self):
        base_line = RavenTestHelper.utc_this_month()
        document_id = "users/ayende"

        with self.store.bulk_insert() as bulk_insert:
            user = User(name="Oren")
            bulk_insert.store_as(user, document_id)

            with bulk_insert.time_series_for(document_id, "Heartrate") as time_series_bulk_insert:
                time_series_bulk_insert.append_single(base_line + timedelta(minutes=1), 59, "watches/fitbit")

                error_message = "There is an already running time series operation, did you forget to close it?"

                self.assertRaisesWithMessageContaining(
                    bulk_insert.store, RuntimeError, error_message, User(name="Oren")
                )

                self.assertRaisesWithMessageContaining(
                    bulk_insert.counters_for("test").increment, RuntimeError, error_message, "1", 1
                )

                self.assertRaisesWithMessageContaining(
                    bulk_insert.time_series_for, RuntimeError, error_message, document_id, "Heartrate"
                )

    def test_can_store_and_read_multiple_timeseries_for_different_documents(self):
        base_line = RavenTestHelper.utc_this_month()
        document_id1 = "users/ayende"
        document_id2 = "users/grisha"

        with self.store.bulk_insert() as bulk_insert:
            user1 = User(name="Oren")
            bulk_insert.store_as(user1, document_id1)
            with bulk_insert.time_series_for(document_id1, "Heartrate") as time_series_bulk_insert:
                time_series_bulk_insert.append_single(base_line + timedelta(minutes=1), 59, "watches/fitbit")

            user2 = User(name="Grisha")
            bulk_insert.store_as(user2, document_id2)

            with bulk_insert.time_series_for(document_id2, "Heartrate") as time_series_bulk_insert2:
                time_series_bulk_insert2.append_single(base_line + timedelta(minutes=1), 59, "watches/fitbit")

        with self.store.bulk_insert() as bulk_insert:
            with bulk_insert.time_series_for(document_id1, "Heartrate") as time_series_bulk_insert:
                time_series_bulk_insert.append_single(base_line + timedelta(minutes=2), 61, "watches/fitbit")

            with bulk_insert.time_series_for(document_id2, "Heartrate") as time_series_bulk_insert:
                time_series_bulk_insert.append_single(base_line + timedelta(minutes=2), 61, "watches/fitbit")

            with bulk_insert.time_series_for(document_id1, "Heartrate") as time_series_bulk_insert:
                time_series_bulk_insert.append_single(base_line + timedelta(minutes=3), 62, "watches/apple-watch")

            with bulk_insert.time_series_for(document_id2, "Heartrate") as time_series_bulk_insert2:
                time_series_bulk_insert2.append_single(base_line + timedelta(minutes=3), 62, "watches/apple-watch")

        def __validate_values(vals: List[TimeSeriesEntry]) -> None:
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

        with self.store.open_session() as session:
            vals = session.time_series_for(document_id1, "Heartrate").get()
            __validate_values(vals)

            vals = session.time_series_for(document_id2, "Heartrate").get()
            __validate_values(vals)

    def test_can_append_a_lot_of_time_series(self):
        number_of_time_series = 10 * 1024

        base_line = RavenTestHelper.utc_this_month()
        document_id = "users/ayende"
        offset = 0

        with self.store.bulk_insert() as bulk_insert:
            user = User(name="Oren")
            bulk_insert.store_as(user, document_id)

            with bulk_insert.time_series_for(document_id, "Heartrate") as time_series_bulk_insert:
                for j in range(number_of_time_series):
                    time_series_bulk_insert.append_single(
                        base_line + timedelta(minutes=offset), offset + 1, "watches/fitbit"
                    )
                    offset += 1

        with self.store.open_session() as session:
            vals = session.time_series_for(document_id, "Heartrate").get()
            self.assertEqual(number_of_time_series, len(vals))

            for i in range(number_of_time_series):
                self.assertEqual(base_line + timedelta(minutes=i), vals[i].timestamp)
                self.assertEqual(1 + i, vals[i].values[0])

        with self.store.open_session() as session:
            user = session.load("users/ayende", User)
            ts_names = session.advanced.get_time_series_for(user)
            self.assertEqual(1, len(ts_names))

            self.assertEqual("Heartrate", ts_names[0])

    def test_using_different_number_of_values_large_to_small(self):
        base_line = RavenTestHelper.utc_this_month()
        document_id = "users/ayende"

        with self.store.bulk_insert() as bulk_insert:
            user = User(name="Oren")
            bulk_insert.store_as(user, document_id)

            with bulk_insert.time_series_for(document_id, "Heartrate") as time_series_bulk_insert:
                time_series_bulk_insert.append(base_line + timedelta(minutes=1), [70, 120, 80], "watches/apple")
                time_series_bulk_insert.append_single(base_line + timedelta(minutes=2), 59, "watches/fitbit")
                time_series_bulk_insert.append_single(base_line + timedelta(minutes=3), 69, "watches/fitbit")

        with self.store.open_session() as session:
            vals = session.time_series_for("users/ayende", "Heartrate").get(None, None)

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
        base_line = RavenTestHelper.utc_this_month()
        document_id = "users/ayende"

        with self.store.open_session() as session:
            user = User(name="Oren")
            session.store(user, document_id)
            session.save_changes()

        offset = 0

        for i in range(10):
            with self.store.bulk_insert() as bulk_insert:
                with bulk_insert.time_series_for(document_id, "Heartrate") as time_series_bulk_insert:
                    for j in range(1000):
                        time_series_bulk_insert.append_single(
                            base_line + timedelta(minutes=offset), 1 + offset, "watches/fitbit"
                        )
                        offset += 1

        with self.store.open_session() as session:
            vals = session.time_series_for("users/ayende", "Heartrate").get(None, None)
            self.assertEqual(10000, len(vals))

            for i in range(10000):
                self.assertEqual(base_line + timedelta(minutes=i), vals[i].timestamp)
                self.assertEqual(1 + i, vals[i].values[0])

    def test_can_get_time_series_names(self):
        base_line = RavenTestHelper.utc_this_month()
        document_id_1 = "users/karmel"
        document_id_2 = "users/ayende"

        with self.store.bulk_insert() as bulk_insert:
            user = User()
            bulk_insert.store_as(user, document_id_1)
            with bulk_insert.time_series_for(document_id_1, "Nasdaq2") as time_series_bulk_insert:
                time_series_bulk_insert.append_single(datetime.utcnow(), 7547.31, "web")

        with self.store.bulk_insert() as bulk_insert:
            with bulk_insert.time_series_for(document_id_1, "Heartrate2") as time_series_bulk_insert2:
                time_series_bulk_insert2.append_single(datetime.utcnow(), 7547.31, "web")

        with self.store.bulk_insert() as bulk_insert:
            user = User()
            bulk_insert.store_as(user, document_id_2)
            with bulk_insert.time_series_for(document_id_2, "Nasdaq") as time_series_bulk_insert:
                time_series_bulk_insert.append_single(datetime.utcnow(), 7547.31, "web")

        with self.store.bulk_insert() as bulk_insert:
            with bulk_insert.time_series_for(document_id_2, "Heartrate") as time_series_bulk_insert:
                time_series_bulk_insert.append_single(datetime.utcnow(), 58, "fitbit")

        with self.store.open_session() as session:
            user = session.load(document_id_2, User)
            ts_names = session.advanced.get_time_series_for(user)
            self.assertEqual(2, len(ts_names))

            # should be sorted
            self.assertEqual("Heartrate", ts_names[0])
            self.assertEqual("Nasdaq", ts_names[1])

        with self.store.open_session() as session:
            user = session.load(document_id_1, User)
            ts_names = session.advanced.get_time_series_for(user)
            self.assertEqual(2, len(ts_names))

            # should be sorted
            self.assertEqual("Heartrate2", ts_names[0])
            self.assertEqual("Nasdaq2", ts_names[1])

        with self.store.bulk_insert() as bulk_insert:
            with bulk_insert.time_series_for(document_id_2, "heartrate") as time_series_bulk_insert:
                time_series_bulk_insert.append_single(base_line + timedelta(minutes=1), 58, "fitbit")

        with self.store.open_session() as session:
            user = session.load("users/ayende", User)
            ts_names = session.advanced.get_time_series_for(user)
            self.assertEqual(2, len(ts_names))

            # should preserve original casing
            self.assertEqual("Heartrate", ts_names[0])
            self.assertEqual("Nasdaq", ts_names[1])

    def test_can_skip_take_time_series(self):
        base_line = RavenTestHelper.utc_this_month()
        document_id = "users/ayende"

        with self.store.bulk_insert() as bulk_insert:
            user = User()
            user.name = "Oren"
            bulk_insert.store_as(user, document_id)

            with bulk_insert.time_series_for(document_id, "Heartrate") as time_series_bulk_insert:
                for i in range(100):
                    time_series_bulk_insert.append_single(base_line + timedelta(minutes=i), 100 + i, "watches/fitbit")
                    i += 1

        with self.store.open_session() as session:
            vals = session.time_series_for("users/ayende", "Heartrate").get(None, None, 5, 20)

            self.assertEqual(20, len(vals))

            for i in range(len(vals)):
                self.assertEqual(base_line + timedelta(minutes=5 + i), vals[i].timestamp)
                self.assertEqual(105 + i, vals[i].value)

    def test_can_store_values_out_of_order(self):
        base_line = RavenTestHelper.utc_this_month()
        document_id = "users/ayende"

        with self.store.open_session() as session:
            user = User()
            user.name = "Oren"
            session.store(user, document_id)
            session.save_changes()

        retries = 1000

        offset = 0

        with self.store.bulk_insert() as bulk_insert:
            with bulk_insert.time_series_for(document_id, "Heartrate") as time_series_bulk_insert:
                for j in range(retries):
                    time_series_bulk_insert.append_single(
                        base_line + timedelta(minutes=offset), offset, "watches/fitbit"
                    )
                    offset += 5

        offset = 1

        with self.store.bulk_insert() as bulk_insert:
            with bulk_insert.time_series_for(document_id, "Heartrate") as time_series_bulk_insert2:
                for j in range(retries):
                    time_series_bulk_insert2.append_single(
                        base_line + timedelta(minutes=offset), offset, "watches/fitbit"
                    )
                    offset += 5

        with self.store.open_session() as session:
            vals = session.time_series_for("users/ayende", "Heartrate").get(None, None)

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

    def test_can_create_simple_time_series_2(self):
        base_line = RavenTestHelper.utc_this_month()
        document_id = "users/ayende"

        with self.store.bulk_insert() as bulk_insert:
            user = User(name="Oren")
            bulk_insert.store_as(user, document_id)

            with bulk_insert.time_series_for(document_id, "Heartrate") as time_series_bulk_insert:
                time_series_bulk_insert.append_single(base_line + timedelta(minutes=1), 59, "watches/fitbit")
                time_series_bulk_insert.append_single(base_line + timedelta(minutes=2), 60, "watches/fitbit")
                time_series_bulk_insert.append_single(base_line + timedelta(minutes=2), 61, "watches/fitbit")

        with self.store.open_session() as session:
            val = session.time_series_for(document_id, "Heartrate").get()
            self.assertEqual(2, len(val))

    def test_can_delete_timestamp(self):
        base_line = RavenTestHelper.utc_this_month()
        document_id = "users/ayende"

        with self.store.bulk_insert() as bulk_insert:
            user = User(name="Oren")
            bulk_insert.store_as(user, document_id)

            with bulk_insert.time_series_for(document_id, "Heartrate") as time_series_bulk_insert:
                time_series_bulk_insert.append_single(base_line + timedelta(minutes=1), 59, "watches/fitbit")
                time_series_bulk_insert.append_single(base_line + timedelta(minutes=2), 69, "watches/fitbit")
                time_series_bulk_insert.append_single(base_line + timedelta(minutes=3), 79, "watches/fitbit")

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

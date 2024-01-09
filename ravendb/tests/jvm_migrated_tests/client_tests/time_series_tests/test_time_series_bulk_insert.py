import uuid
from datetime import timedelta

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

from datetime import datetime, timedelta

from ravendb.infrastructure.orders import Company, Order
from ravendb.tests.test_base import TestBase, User


class TestTimeSeriesIncludes(TestBase):
    def setUp(self):
        super(TestTimeSeriesIncludes, self).setUp()

    def test_should_cache_empty_time_series_ranges(self):
        document_id = "users/gracjan"
        base_line = datetime(2023, 8, 20, 21, 30)

        with self.store.open_session() as session:
            user = User(name="Gracjan")
            session.store(user, document_id)
            session.save_changes()

        with self.store.open_session() as session:
            tsf = session.time_series_for(document_id, "Heartrate")
            for i in range(360):
                tsf.append_single(base_line + timedelta(seconds=i * 10), 6, "watches/fitbit")

            session.save_changes()

        with self.store.open_session() as session:
            user = session.load(
                document_id,
                User,
                lambda i: i.include_time_series(
                    "Heartrate", base_line - timedelta(minutes=30), base_line - timedelta(minutes=10)
                ),
            )

            self.assertEqual(1, session.advanced.number_of_requests)

            self.assertEqual("Gracjan", user.name)

            # should not go to server

            vals = session.time_series_for(document_id, "Heartrate").get(
                base_line - timedelta(minutes=30), base_line - timedelta(minutes=10)
            )
            self.assertEqual(1, session.advanced.number_of_requests)

            self.assertEqual(0, len(vals))

            cache = session.time_series_by_doc_id.get(document_id)
            ranges = cache.get("Heartrate")
            self.assertEqual(1, len(ranges))

            self.assertEqual(0, len(ranges[0].entries))

            self.assertEqual(base_line - timedelta(minutes=30), ranges[0].from_date)
            self.assertEqual(base_line - timedelta(minutes=10), ranges[0].to_date)

            # should not go to server

            vals = session.time_series_for(document_id, "Heartrate").get(
                base_line - timedelta(minutes=25), base_line - timedelta(minutes=15)
            )

            self.assertEqual(1, session.advanced.number_of_requests)

            self.assertEqual(0, len(vals))

            session.advanced.evict(user)

            user = session.load(
                document_id,
                User,
                lambda i: i.include_time_series(
                    "BloodPressure", base_line + timedelta(minutes=10), base_line + timedelta(minutes=30)
                ),
            )

            self.assertEqual(2, session.advanced.number_of_requests)

            # should not go to server

            vals = session.time_series_for(document_id, "BloodPressure").get(
                base_line + timedelta(minutes=10), base_line + timedelta(minutes=30)
            )

            self.assertEqual(2, session.advanced.number_of_requests)

            self.assertEqual(0, len(vals))

            cache = session.time_series_by_doc_id.get(document_id)
            ranges = cache.get("BloodPressure")
            self.assertEqual(1, len(ranges))
            self.assertEqual(0, len(ranges[0].entries))

            self.assertEqual(base_line + timedelta(minutes=10), ranges[0].from_date)
            self.assertEqual(base_line + timedelta(minutes=30), ranges[0].to_date)

    def test_session_load_with_include_time_series(self):
        base_line = datetime(2023, 8, 20, 21, 30)

        with self.store.open_session() as session:
            company = Company(name="HR")
            session.store(company, "companies/1-A")

            order = Order(company="companies/1-A")
            session.store(order, "orders/1-A")

            tsf = session.time_series_for("orders/1-A", "Heartrate")
            tsf.append_single(base_line, 67, "watches/apple")
            tsf.append_single(base_line + timedelta(minutes=5), 64, "watches/apple")
            tsf.append_single(base_line + timedelta(minutes=10), 65, "watches/fitbit")

            session.save_changes()

        with self.store.open_session() as session:
            order = session.load(
                "orders/1-A", Order, lambda i: i.include_documents("company").include_time_series("Heartrate")
            )

            company = session.load(order.company, Company)
            self.assertEqual("HR", company.name)

            # should not go to server
            values = session.time_series_for_entity(order, "Heartrate").get()

            self.assertEqual(3, len(values))

            self.assertEqual(1, len(values[0].values))
            self.assertEqual(67, values[0].values[0])
            self.assertEqual("watches/apple", values[0].tag)
            self.assertEqual(base_line, values[0].timestamp)

            self.assertEqual(1, len(values[1].values))
            self.assertEqual(64, values[1].values[0])
            self.assertEqual("watches/apple", values[1].tag)
            self.assertEqual(base_line + timedelta(minutes=5), values[1].timestamp)

            self.assertEqual(1, len(values[2].values))
            self.assertEqual(65, values[2].values[0])
            self.assertEqual("watches/fitbit", values[2].tag)
            self.assertEqual(base_line + timedelta(minutes=10), values[2].timestamp)

            self.assertEqual(1, session.advanced.number_of_requests)

    def test_include_time_series_and_update_existing_range_in_cache(self):
        document_id = "users/gracjan"
        base_line = datetime(2023, 8, 20, 21, 30)

        with self.store.open_session() as session:
            user = User(name="Gracjan")
            session.store(user, document_id)
            session.save_changes()

        with self.store.open_session() as session:
            tsf = session.time_series_for(document_id, "Heartrate")
            for i in range(360):
                tsf.append_single(base_line + timedelta(seconds=10 * i), 6, "watches/fitbit")

            session.save_changes()

        with self.store.open_session() as session:
            vals = session.time_series_for(document_id, "Heartrate").get(
                base_line + timedelta(minutes=2), base_line + timedelta(minutes=10)
            )

            self.assertEqual(1, session.advanced.number_of_requests)

            self.assertEqual(49, len(vals))
            self.assertEqual(base_line + timedelta(minutes=2), vals[0].timestamp)
            self.assertEqual(base_line + timedelta(minutes=10), vals[48].timestamp)

            session.time_series_for(document_id, "Heartrate").append_single(
                base_line + timedelta(minutes=3, seconds=3), 6, "watches/fitbit"
            )
            session.save_changes()

            self.assertEqual(2, session.advanced.number_of_requests)

            user = session.load(
                document_id,
                User,
                lambda i: i.include_time_series(
                    "Heartrate", base_line + timedelta(minutes=3), base_line + timedelta(minutes=5)
                ),
            )

            self.assertEqual(3, session.advanced.number_of_requests)

            # should not go to server

            vals = session.time_series_for(document_id, "Heartrate").get(
                base_line + timedelta(minutes=3), base_line + timedelta(minutes=5)
            )

            self.assertEqual(14, len(vals))

            self.assertEqual(base_line + timedelta(minutes=3), vals[0].timestamp)
            self.assertEqual(base_line + timedelta(minutes=3, seconds=3), vals[1].timestamp)
            self.assertEqual(base_line + timedelta(minutes=5), vals[13].timestamp)

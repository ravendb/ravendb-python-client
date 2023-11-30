from datetime import datetime, timedelta

from ravendb.documents.session.time_series import TimeSeriesRangeType
from ravendb.infrastructure.orders import Company, Order
from ravendb.primitives.time_series import TimeValue
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

    def test_include_multiple_time_series(self):
        document_id = "users/gracjan"
        base_line = datetime(2023, 8, 20, 21, 30)

        with self.store.open_session() as session:
            user = User(name="Gracjan")
            session.store(user, document_id)
            session.save_changes()

        with self.store.open_session() as session:
            for i in range(360):
                session.time_series_for(document_id, "Heartrate").append_single(
                    base_line + timedelta(seconds=i * 10), 6, "watches/fitbit"
                )
                session.time_series_for(document_id, "BloodPressure").append_single(
                    base_line + timedelta(seconds=i * 10), 66, "watches/fitbit"
                )
                session.time_series_for(document_id, "Nasdaq").append_single(
                    base_line + timedelta(seconds=i * 10), 8097.23, "nasdaq.com"
                )

            session.save_changes()

        with self.store.open_session() as session:
            session.load(
                document_id,
                User,
                lambda i: i.include_time_series(
                    "Heartrate", base_line + timedelta(minutes=3), base_line + timedelta(minutes=5)
                )
                .include_time_series(
                    "BloodPressure", base_line + timedelta(minutes=40), base_line + timedelta(minutes=45)
                )
                .include_time_series("Nasdaq", base_line + timedelta(minutes=15), base_line + timedelta(minutes=25)),
            )

            self.assertEqual(1, session.advanced.number_of_requests)

            self.assertEqual("Gracjan", user.name)

            # should not go to server

            vals = session.time_series_for(document_id, "Heartrate").get(
                base_line + timedelta(minutes=3), base_line + timedelta(minutes=5)
            )
            self.assertEqual(1, session.advanced.number_of_requests)

            self.assertEqual(13, len(vals))

            self.assertEqual(base_line + timedelta(minutes=3), vals[0].timestamp)
            self.assertEqual(base_line + timedelta(minutes=5), vals[12].timestamp)

            # should not go to server

            vals = session.time_series_for(document_id, "BloodPressure").get(
                base_line + timedelta(minutes=40), base_line + timedelta(minutes=45)
            )
            self.assertEqual(1, session.advanced.number_of_requests)

            self.assertEqual(31, len(vals))

            self.assertEqual(base_line + timedelta(minutes=40), vals[0].timestamp)
            self.assertEqual(base_line + timedelta(minutes=45), vals[30].timestamp)

            # should not go to server

            vals = session.time_series_for(document_id, "Nasdaq").get(
                base_line + timedelta(minutes=15), base_line + timedelta(minutes=25)
            )
            self.assertEqual(1, session.advanced.number_of_requests)

            self.assertEqual(61, len(vals))

            self.assertEqual(base_line + timedelta(minutes=15), vals[0].timestamp)
            self.assertEqual(base_line + timedelta(minutes=25), vals[60].timestamp)

    def test_include_time_series_and_merge_with_existing_ranges_in_cache(self):
        document_id = "users/gracjan"
        base_line = datetime(2023, 8, 20, 21, 30)
        ts_name = "Heartrate"

        with self.store.open_session() as session:
            session.store(User(name="Gracjan"), document_id)
            session.save_changes()

        with self.store.open_session() as session:
            tsf = session.time_series_for(document_id, ts_name)
            for i in range(360):
                tsf.append_single(base_line + timedelta(seconds=10 * i), 6, "watches/fitbit")

            session.save_changes()

        with self.store.open_session() as session:
            vals = session.time_series_for(document_id, ts_name).get(
                base_line + timedelta(minutes=2), base_line + timedelta(minutes=10)
            )
            self.assertEqual(1, session.advanced.number_of_requests)
            self.assertEqual(49, len(vals))
            self.assertEqual(base_line + timedelta(minutes=2), vals[0].timestamp)
            self.assertEqual(base_line + timedelta(minutes=10), vals[48].timestamp)

            user = session.load(
                document_id,
                User,
                lambda i: i.include_time_series(
                    ts_name, base_line + timedelta(minutes=40), base_line + timedelta(minutes=50)
                ),
            )
            self.assertEqual(2, session.advanced.number_of_requests)

            # should not go to server
            vals = session.time_series_for(document_id, ts_name).get(
                base_line + timedelta(minutes=40), base_line + timedelta(minutes=50)
            )
            self.assertEqual(2, session.advanced.number_of_requests)

            self.assertEqual(61, len(vals))

            self.assertEqual(base_line + timedelta(minutes=40), vals[0].timestamp)
            self.assertEqual(base_line + timedelta(minutes=50), vals[60].timestamp)

            cache = session.time_series_by_doc_id.get(document_id)
            self.assertIsNotNone(cache)
            ranges = cache.get(ts_name)
            self.assertEqual(2, len(ranges))

            self.assertEqual(base_line + timedelta(minutes=2), ranges[0].from_date)
            self.assertEqual(base_line + timedelta(minutes=10), ranges[0].to_date)
            self.assertEqual(base_line + timedelta(minutes=40), ranges[1].from_date)
            self.assertEqual(base_line + timedelta(minutes=50), ranges[1].to_date)

            # we intentionally evict just the document (without it's TS data),
            # so that Load request will go to server

            session.documents_by_entity.evict(user)
            session.documents_by_id.remove(document_id)

            # should go to server to get [0, 2] and merge it into existing [2, 10]
            user = session.load(
                document_id, User, lambda i: i.include_time_series(ts_name, base_line, base_line + timedelta(minutes=2))
            )

            self.assertEqual(3, session.advanced.number_of_requests)

            # should not go to server

            vals = session.time_series_for(document_id, ts_name).get(base_line, base_line + timedelta(minutes=2))

            self.assertEqual(3, session.advanced.number_of_requests)

            self.assertEqual(13, len(vals))
            self.assertEqual(base_line, vals[0].timestamp)
            self.assertEqual(base_line + timedelta(minutes=2), vals[12].timestamp)

            self.assertEqual(2, len(ranges))
            self.assertEqual(base_line + timedelta(minutes=0), ranges[0].from_date)
            self.assertEqual(base_line + timedelta(minutes=10), ranges[0].to_date)
            self.assertEqual(base_line + timedelta(minutes=40), ranges[1].from_date)
            self.assertEqual(base_line + timedelta(minutes=50), ranges[1].to_date)

            # evict just the document
            session.documents_by_entity.evict(user)
            session.documents_by_id.remove(document_id)

            # should go to server to get [10, 16] and merge it into existing [0, 10]
            user = session.load(
                document_id,
                User,
                lambda i: i.include_time_series(
                    ts_name, base_line + timedelta(minutes=10), base_line + timedelta(minutes=16)
                ),
            )

            self.assertEqual(4, session.advanced.number_of_requests)

            # should not go to server
            vals = session.time_series_for(document_id, ts_name).get(
                base_line + timedelta(minutes=10), base_line + timedelta(minutes=16)
            )
            self.assertEqual(4, session.advanced.number_of_requests)

            self.assertEqual(37, len(vals))
            self.assertEqual(base_line + timedelta(minutes=10), vals[0].timestamp)
            self.assertEqual(base_line + timedelta(minutes=16), vals[36].timestamp)

            self.assertEqual(2, len(ranges))

            self.assertEqual(base_line, ranges[0].from_date)
            self.assertEqual(base_line + timedelta(minutes=16), ranges[0].to_date)
            self.assertEqual(base_line + timedelta(minutes=40), ranges[1].from_date)
            self.assertEqual(base_line + timedelta(minutes=50), ranges[1].to_date)

            # evict just the document
            session.documents_by_entity.evict(user)
            session.documents_by_id.remove(document_id)

            # should go to server to get range [17, 19]
            # and add it to cache in between [10, 16] and [40, 50]

            user = session.load(
                document_id,
                User,
                lambda i: i.include_time_series(
                    ts_name, base_line + timedelta(minutes=17), base_line + timedelta(minutes=19)
                ),
            )

            self.assertEqual(5, session.advanced.number_of_requests)

            # should not go to server

            vals = session.time_series_for(document_id, ts_name).get(
                base_line + timedelta(minutes=17), base_line + timedelta(minutes=19)
            )

            self.assertEqual(5, session.advanced.number_of_requests)

            self.assertEqual(13, len(vals))
            self.assertEqual(base_line + timedelta(minutes=17), vals[0].timestamp)
            self.assertEqual(base_line + timedelta(minutes=19), vals[12].timestamp)

            self.assertEqual(3, len(ranges))
            self.assertEqual(base_line, ranges[0].from_date)
            self.assertEqual(base_line + timedelta(minutes=16), ranges[0].to_date)
            self.assertEqual(base_line + timedelta(minutes=17), ranges[1].from_date)
            self.assertEqual(base_line + timedelta(minutes=19), ranges[1].to_date)
            self.assertEqual(base_line + timedelta(minutes=40), ranges[2].from_date)
            self.assertEqual(base_line + timedelta(minutes=50), ranges[2].to_date)

            # evict just the document
            session.documents_by_entity.evict(user)
            session.documents_by_id.remove(document_id)

            # should go to server to get range [19, 40]
            # and merge the result with existing ranges [17, 19] and [40, 50]
            # into single range [17, 50]

            user = session.load(
                document_id,
                User,
                lambda i: i.include_time_series(
                    ts_name, base_line + timedelta(minutes=18), base_line + timedelta(minutes=48)
                ),
            )

            self.assertEqual(6, session.advanced.number_of_requests)

            # should not go to server

            vals = session.time_series_for(document_id, ts_name).get(
                base_line + timedelta(minutes=18), base_line + timedelta(minutes=48)
            )

            self.assertEqual(6, session.advanced.number_of_requests)

            self.assertEqual(181, len(vals))
            self.assertEqual(base_line + timedelta(minutes=18), vals[0].timestamp)
            self.assertEqual(base_line + timedelta(minutes=48), vals[180].timestamp)

            self.assertEqual(2, len(ranges))
            self.assertEqual(base_line, ranges[0].from_date)
            self.assertEqual(base_line + timedelta(minutes=16), ranges[0].to_date)
            self.assertEqual(base_line + timedelta(minutes=17), ranges[1].from_date)
            self.assertEqual(base_line + timedelta(minutes=50), ranges[1].to_date)

            # evict just the document
            session.documents_by_entity.evict(user)
            session.documents_by_id.remove(document_id)

            # should go to server to get range [12, 22]
            # and merge the result with existing ranges [0, 16] and [17, 50]
            # into single range [0, 50]

            user = session.load(
                document_id,
                User,
                lambda i: i.include_time_series(
                    ts_name, base_line + timedelta(minutes=12), base_line + timedelta(minutes=22)
                ),
            )

            self.assertEqual(7, session.advanced.number_of_requests)

            # should not go to server

            vals = session.time_series_for(document_id, ts_name).get(
                base_line + timedelta(minutes=12), base_line + timedelta(minutes=22)
            )

            self.assertEqual(7, session.advanced.number_of_requests)

            self.assertEqual(61, len(vals))
            self.assertEqual(base_line + timedelta(minutes=12), vals[0].timestamp)
            self.assertEqual(base_line + timedelta(minutes=22), vals[60].timestamp)

            self.assertEqual(1, len(ranges))
            self.assertEqual(base_line, ranges[0].from_date)
            self.assertEqual(base_line + timedelta(minutes=50), ranges[0].to_date)

            # evict just the document
            session.documents_by_entity.evict(user)
            session.documents_by_id.remove(document_id)

            # should go to server to get range [50, ∞]
            # and merge the result with existing range [0, 50] into single range [0, ∞]

            user = session.load(
                document_id,
                User,
                lambda i: i.include_time_series_by_range_type_and_time(
                    ts_name, TimeSeriesRangeType.LAST, TimeValue.of_minutes(10)
                ),
            )

            self.assertEqual(8, session.advanced.number_of_requests)

            # should not go to server

            vals = session.time_series_for(document_id, ts_name).get(base_line + timedelta(minutes=50), None)

            self.assertEqual(8, session.advanced.number_of_requests)

            # should not go to server

            vals = session.time_series_for(document_id, ts_name).get(base_line + timedelta(minutes=50), None)

            self.assertEqual(8, session.advanced.number_of_requests)

            self.assertEqual(60, len(vals))

            self.assertEqual(base_line + timedelta(minutes=50), vals[0].timestamp)
            self.assertEqual(base_line + timedelta(minutes=59, seconds=50), vals[59].timestamp)

            self.assertEqual(1, len(ranges))
            self.assertEqual(base_line, ranges[0].from_date)
            self.assertEqual(None, ranges[0].to_date)

    def test_query_with_include_time_series(self):
        document_id = "users/gracjan"
        base_line = datetime(2023, 8, 20, 21, 30)
        ts_name = "Heartrate"
        tag = "watches/fitbit"

        with self.store.open_session() as session:
            session.store(User(name="Gracjan"), document_id)
            session.save_changes()

        with self.store.open_session() as session:
            tsf = session.time_series_for(document_id, ts_name)

            for i in range(360):
                tsf.append_single(base_line + timedelta(seconds=i * 10), 67, tag)

            session.save_changes()

        with self.store.open_session() as session:
            query = session.query(object_type=User).include(lambda i: i.include_time_series(ts_name))

            result = list(query)

            self.assertEqual(1, session.advanced.number_of_requests)

            self.assertEqual("Gracjan", result[0].name)

            # should not go to server

            vals = session.time_series_for(document_id, ts_name).get(base_line, base_line + timedelta(minutes=30))

            self.assertEqual(1, session.advanced.number_of_requests)

            self.assertEqual(181, len(vals))
            self.assertEqual(base_line, vals[0].timestamp)
            self.assertEqual(tag, vals[0].tag)
            self.assertEqual(67, vals[0].values[0])
            self.assertEqual(base_line + timedelta(minutes=30), vals[180].timestamp)

    def test_can_load_async_with_include_time_series_last_range_by_count(self):
        company_id = "companies/1-A"
        order_id = "orders/1-A"
        base_line = datetime(2023, 8, 20, 21, 30)
        ts_name = "Heartrate"
        tag = "watches/fitbit"

        with self.store.open_session() as session:
            session.store(Company(name="HR"), company_id)
            session.store(Order(company=company_id), order_id)
            tsf = session.time_series_for(order_id, ts_name)

            for i in range(15):
                tsf.append_single(base_line - timedelta(minutes=i), i, tag)

            session.save_changes()

        with self.store.open_session() as session:
            order = session.load(
                order_id,
                Order,
                lambda i: i.include_documents("company").include_time_series_by_range_type_and_count(
                    ts_name, TimeSeriesRangeType.LAST, 11
                ),
            )
            self.assertEqual(1, session.advanced.number_of_requests)

            # should not go to server

            company = session.load(order.company, Company)

            self.assertEqual(1, session.advanced.number_of_requests)
            self.assertEqual("HR", company.name)

            # should not go to server
            values = session.time_series_for(order_id, ts_name).get(base_line - timedelta(minutes=10), None)

            self.assertEqual(1, session.advanced.number_of_requests)
            self.assertEqual(11, len(values))

            for i in range(len(values)):
                self.assertEqual(1, len(values[i].values))
                self.assertEqual(len(values) - 1 - i, values[i].values[0])
                self.assertEqual(tag, values[i].tag)
                self.assertEqual(base_line - timedelta(minutes=len(values) - 1 - i), values[i].timestamp)

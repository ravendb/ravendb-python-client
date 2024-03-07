from datetime import datetime, timedelta

from ravendb.documents.session.time_series import TimeSeriesRangeType
from ravendb.infrastructure.orders import Company, Order
from ravendb.primitives.constants import int_max
from ravendb.primitives.time_series import TimeValue
from ravendb.tests.test_base import TestBase

document_id = "users/gracjan"
company_id = "companies/1-A"
order_id = "orders/1-A"
base_line = datetime(2023, 8, 20, 21, 30)
ts_name1 = "Heartrate"
ts_name2 = "Speedrate"
tag1 = "watches/fitbit"
tag2 = "watches/apple"
tag3 = "watches/bitfit"


class User:
    def __init__(self, Id: str = None, name: str = None, works_at: str = None):
        self.Id = Id
        self.name = name
        self.works_at = works_at


class TestTimeSeriesIncludes(TestBase):
    def setUp(self):
        super(TestTimeSeriesIncludes, self).setUp()

    def test_should_cache_empty_time_series_ranges(self):
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
        with self.store.open_session() as session:
            session.store(User(name="Gracjan"), document_id)
            session.save_changes()

        with self.store.open_session() as session:
            tsf = session.time_series_for(document_id, ts_name1)
            for i in range(360):
                tsf.append_single(base_line + timedelta(seconds=10 * i), 6, "watches/fitbit")

            session.save_changes()

        with self.store.open_session() as session:
            vals = session.time_series_for(document_id, ts_name1).get(
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
                    ts_name1, base_line + timedelta(minutes=40), base_line + timedelta(minutes=50)
                ),
            )
            self.assertEqual(2, session.advanced.number_of_requests)

            # should not go to server
            vals = session.time_series_for(document_id, ts_name1).get(
                base_line + timedelta(minutes=40), base_line + timedelta(minutes=50)
            )
            self.assertEqual(2, session.advanced.number_of_requests)

            self.assertEqual(61, len(vals))

            self.assertEqual(base_line + timedelta(minutes=40), vals[0].timestamp)
            self.assertEqual(base_line + timedelta(minutes=50), vals[60].timestamp)

            cache = session.time_series_by_doc_id.get(document_id)
            self.assertIsNotNone(cache)
            ranges = cache.get(ts_name1)
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
                document_id,
                User,
                lambda i: i.include_time_series(ts_name1, base_line, base_line + timedelta(minutes=2)),
            )

            self.assertEqual(3, session.advanced.number_of_requests)

            # should not go to server

            vals = session.time_series_for(document_id, ts_name1).get(base_line, base_line + timedelta(minutes=2))

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
                    ts_name1, base_line + timedelta(minutes=10), base_line + timedelta(minutes=16)
                ),
            )

            self.assertEqual(4, session.advanced.number_of_requests)

            # should not go to server
            vals = session.time_series_for(document_id, ts_name1).get(
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
                    ts_name1, base_line + timedelta(minutes=17), base_line + timedelta(minutes=19)
                ),
            )

            self.assertEqual(5, session.advanced.number_of_requests)

            # should not go to server

            vals = session.time_series_for(document_id, ts_name1).get(
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
                    ts_name1, base_line + timedelta(minutes=18), base_line + timedelta(minutes=48)
                ),
            )

            self.assertEqual(6, session.advanced.number_of_requests)

            # should not go to server

            vals = session.time_series_for(document_id, ts_name1).get(
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
                    ts_name1, base_line + timedelta(minutes=12), base_line + timedelta(minutes=22)
                ),
            )

            self.assertEqual(7, session.advanced.number_of_requests)

            # should not go to server

            vals = session.time_series_for(document_id, ts_name1).get(
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
                    ts_name1, TimeSeriesRangeType.LAST, TimeValue.of_minutes(10)
                ),
            )

            self.assertEqual(8, session.advanced.number_of_requests)

            # should not go to server

            vals = session.time_series_for(document_id, ts_name1).get(base_line + timedelta(minutes=50), None)

            self.assertEqual(8, session.advanced.number_of_requests)

            # should not go to server

            vals = session.time_series_for(document_id, ts_name1).get(base_line + timedelta(minutes=50), None)

            self.assertEqual(8, session.advanced.number_of_requests)

            self.assertEqual(60, len(vals))

            self.assertEqual(base_line + timedelta(minutes=50), vals[0].timestamp)
            self.assertEqual(base_line + timedelta(minutes=59, seconds=50), vals[59].timestamp)

            self.assertEqual(1, len(ranges))
            self.assertEqual(base_line, ranges[0].from_date)
            self.assertEqual(None, ranges[0].to_date)

    def test_query_with_include_time_series(self):
        with self.store.open_session() as session:
            session.store(User(name="Gracjan"), document_id)
            session.save_changes()

        with self.store.open_session() as session:
            tsf = session.time_series_for(document_id, ts_name1)

            for i in range(360):
                tsf.append_single(base_line + timedelta(seconds=i * 10), 67, tag1)

            session.save_changes()

        with self.store.open_session() as session:
            query = session.query(object_type=User).include(lambda i: i.include_time_series(ts_name1))

            result = list(query)

            self.assertEqual(1, session.advanced.number_of_requests)

            self.assertEqual("Gracjan", result[0].name)

            # should not go to server

            vals = session.time_series_for(document_id, ts_name1).get(base_line, base_line + timedelta(minutes=30))

            self.assertEqual(1, session.advanced.number_of_requests)

            self.assertEqual(181, len(vals))
            self.assertEqual(base_line, vals[0].timestamp)
            self.assertEqual(tag1, vals[0].tag)
            self.assertEqual(67, vals[0].values[0])
            self.assertEqual(base_line + timedelta(minutes=30), vals[180].timestamp)

    def test_can_load_async_with_include_time_series_last_range_by_count(self):
        with self.store.open_session() as session:
            session.store(Company(name="HR"), company_id)
            session.store(Order(company=company_id), order_id)
            tsf = session.time_series_for(order_id, ts_name1)

            for i in range(15):
                tsf.append_single(base_line - timedelta(minutes=i), i, tag1)

            session.save_changes()

        with self.store.open_session() as session:
            order = session.load(
                order_id,
                Order,
                lambda i: i.include_documents("company").include_time_series_by_range_type_and_count(
                    ts_name1, TimeSeriesRangeType.LAST, 11
                ),
            )
            self.assertEqual(1, session.advanced.number_of_requests)

            # should not go to server

            company = session.load(order.company, Company)

            self.assertEqual(1, session.advanced.number_of_requests)
            self.assertEqual("HR", company.name)

            # should not go to server
            values = session.time_series_for(order_id, ts_name1).get(base_line - timedelta(minutes=10), None)

            self.assertEqual(1, session.advanced.number_of_requests)
            self.assertEqual(11, len(values))

            for i in range(len(values)):
                self.assertEqual(1, len(values[i].values))
                self.assertEqual(len(values) - 1 - i, values[i].values[0])
                self.assertEqual(tag1, values[i].tag)
                self.assertEqual(base_line - timedelta(minutes=len(values) - 1 - i), values[i].timestamp)

    def test_can_load_async_with_include_array_of_time_series_last_range_by_time(self):
        self.can_load_async_with_include_array_of_time_series_last_range(True)

    def test_can_load_async_with_include_array_of_time_series_last_range_by_count(self):
        self.can_load_async_with_include_array_of_time_series_last_range(False)

    def can_load_async_with_include_array_of_time_series_last_range(self, by_time: bool) -> None:
        with self.store.open_session() as session:
            session.store(Company(name="HR"), company_id)
            session.store(Order(company=company_id), order_id)

            tsf = session.time_series_for(order_id, ts_name1)
            tsf.append_single(base_line, 67, tag2)
            tsf.append_single(base_line - timedelta(minutes=5), 64, tag2)
            tsf.append_single(base_line - timedelta(minutes=10), 65, tag1)

            tsf2 = session.time_series_for(order_id, ts_name2)
            tsf2.append_single(base_line - timedelta(minutes=15), 6, tag3)
            tsf2.append_single(base_line - timedelta(minutes=10), 7, tag3)
            tsf2.append_single(base_line - timedelta(minutes=9), 7, tag3)
            tsf2.append_single(base_line - timedelta(minutes=8), 6, tag3)

            session.save_changes()

        with self.store.open_session() as session:
            order = (
                session.load(
                    order_id,
                    Order,
                    lambda i: i.include_documents("company").include_array_of_time_series_by_range_type_and_time(
                        [ts_name1, ts_name2], TimeSeriesRangeType.LAST, TimeValue.of_minutes(10)
                    ),
                )
                if by_time
                else session.load(
                    order_id,
                    Order,
                    lambda i: i.include_documents("company").include_array_of_time_series_by_range_type_and_count(
                        [ts_name1, ts_name2], TimeSeriesRangeType.LAST, 3
                    ),
                )
            )

            self.assertEqual(1, session.advanced.number_of_requests)

            # should not go to server
            company = session.load(order.company, Company)

            self.assertEqual(1, session.advanced.number_of_requests)
            self.assertEqual("HR", company.name)

            # should not go to server
            heartrate_values = session.time_series_for_entity(order, ts_name1).get(
                base_line - timedelta(minutes=10), None
            )
            self.assertEqual(1, session.advanced.number_of_requests)
            self.assertEqual(3, len(heartrate_values))

            self.assertEqual(1, len(heartrate_values[0].values))
            self.assertEqual(65, heartrate_values[0].values[0])
            self.assertEqual(tag1, heartrate_values[0].tag)
            self.assertEqual(base_line - timedelta(minutes=10), heartrate_values[0].timestamp)

            self.assertEqual(1, len(heartrate_values[1].values))
            self.assertEqual(64, heartrate_values[1].values[0])
            self.assertEqual(tag2, heartrate_values[1].tag)
            self.assertEqual(base_line - timedelta(minutes=5), heartrate_values[1].timestamp)

            self.assertEqual(1, len(heartrate_values[2].values))
            self.assertEqual(67, heartrate_values[2].values[0])
            self.assertEqual(tag2, heartrate_values[2].tag)
            self.assertEqual(base_line, heartrate_values[2].timestamp)

            # should not go to server
            speedrate_values = session.time_series_for_entity(order, ts_name2).get(
                base_line - timedelta(minutes=10), None
            )

            self.assertEqual(1, session.advanced.number_of_requests)
            self.assertEqual(3, len(speedrate_values))

            self.assertEqual(1, len(speedrate_values[0].values))
            self.assertEqual(7, speedrate_values[0].values[0])
            self.assertEqual(tag3, speedrate_values[0].tag)
            self.assertEqual(base_line - timedelta(minutes=10), speedrate_values[0].timestamp)

            self.assertEqual(1, len(speedrate_values[1].values))
            self.assertEqual(7, speedrate_values[1].values[0])
            self.assertEqual(tag3, speedrate_values[1].tag)
            self.assertEqual(base_line - timedelta(minutes=9), speedrate_values[1].timestamp)

            self.assertEqual(1, len(speedrate_values[2].values))
            self.assertEqual(6, speedrate_values[2].values[0])
            self.assertEqual(tag3, speedrate_values[2].tag)
            self.assertEqual(base_line - timedelta(minutes=8), speedrate_values[2].timestamp)

    def test_can_load_async_with_include_all_time_series_last_range_by_count(self):
        with self.store.open_session() as session:
            session.store(Company(name="HR"), company_id)
            session.store(Order(company=company_id), order_id)
            tsf = session.time_series_for(order_id, ts_name1)
            for i in range(15):
                tsf.append_single(base_line - timedelta(minutes=i), i, tag1)
            tsf2 = session.time_series_for(order_id, ts_name2)
            for i in range(15):
                tsf2.append_single(base_line - timedelta(minutes=i), i, tag1)

            session.save_changes()

        with self.store.open_session() as session:
            order = session.load(
                order_id,
                Order,
                lambda i: i.include_documents("company").include_all_time_series_by_count(TimeSeriesRangeType.LAST, 11),
            )

            self.assertEqual(1, session.advanced.number_of_requests)

            # should not go to server
            company = session.load(order.company, Company)

            self.assertEqual(1, session.advanced.number_of_requests)

            self.assertEqual("HR", company.name)

            # should not go to server
            heartrate_values = session.time_series_for_entity(order, ts_name1).get(
                base_line - timedelta(minutes=10), None
            )
            self.assertEqual(11, len(heartrate_values))
            self.assertEqual(1, session.advanced.number_of_requests)

            speedrate_values = session.time_series_for_entity(order, ts_name2).get(
                base_line - timedelta(minutes=10), None
            )

            self.assertEqual(11, len(speedrate_values))
            self.assertEqual(1, session.advanced.number_of_requests)

            for i in range(len(heartrate_values)):
                self.assertEqual(1, len(heartrate_values[i].values))
                self.assertEqual(len(heartrate_values) - 1 - i, heartrate_values[i].values[0])
                self.assertEqual(tag1, heartrate_values[i].tag)
                self.assertEqual(
                    base_line - timedelta(minutes=len(heartrate_values) - 1 - i), heartrate_values[i].timestamp
                )

            for i in range(len(speedrate_values)):
                self.assertEqual(1, len(speedrate_values[i].values))
                self.assertEqual(len(speedrate_values) - 1 - i, speedrate_values[i].values[0])
                self.assertEqual(tag1, speedrate_values[i].tag)
                self.assertEqual(
                    base_line - timedelta(minutes=len(speedrate_values) - 1 - i), speedrate_values[i].timestamp
                )

    def test_can_load_async_with_include_all_time_series_last_range_by_type(self):
        with self.store.open_session() as session:
            session.store(Company(name="HR"), company_id)
            session.store(Order(company=company_id), order_id)
            tsf = session.time_series_for(order_id, ts_name1)
            for i in range(15):
                tsf.append_single(base_line - timedelta(minutes=i), i, tag1)
            tsf2 = session.time_series_for(order_id, ts_name2)
            for i in range(15):
                tsf2.append_single(base_line - timedelta(minutes=i), i, tag1)

            session.save_changes()

        with self.store.open_session() as session:
            order = session.load(
                order_id,
                Order,
                lambda i: i.include_documents("company").include_all_time_series_by_time(
                    TimeSeriesRangeType.LAST, TimeValue.of_minutes(10)
                ),
            )

            # should not go to server
            company = session.load(order.company, Company)

            self.assertEqual(1, session.advanced.number_of_requests)

            self.assertEqual("HR", company.name)

            # should not go to server
            heartrate_values = session.time_series_for_entity(order, ts_name1).get(
                base_line - timedelta(minutes=10), None
            )
            self.assertEqual(11, len(heartrate_values))
            self.assertEqual(1, session.advanced.number_of_requests)

            speedrate_values = session.time_series_for_entity(order, ts_name2).get(
                base_line - timedelta(minutes=10), None
            )

            self.assertEqual(11, len(speedrate_values))
            self.assertEqual(1, session.advanced.number_of_requests)

            for i in range(len(heartrate_values)):
                self.assertEqual(1, len(heartrate_values[i].values))
                self.assertEqual(len(heartrate_values) - 1 - i, heartrate_values[i].values[0])
                self.assertEqual(tag1, heartrate_values[i].tag)
                self.assertEqual(
                    base_line - timedelta(minutes=len(heartrate_values) - 1 - i), heartrate_values[i].timestamp
                )

            for i in range(len(speedrate_values)):
                self.assertEqual(1, len(speedrate_values[i].values))
                self.assertEqual(len(speedrate_values) - 1 - i, speedrate_values[i].values[0])
                self.assertEqual(tag1, speedrate_values[i].tag)
                self.assertEqual(
                    base_line - timedelta(minutes=len(speedrate_values) - 1 - i), speedrate_values[i].timestamp
                )

    def test_multi_load_with_include_time_series(self):
        with self.store.open_session() as session:
            session.store(User(name="Oren"), "users/ayende")
            session.store(User(name="Gracjan"), "users/gracjan")

            session.save_changes()

        with self.store.open_session() as session:
            tsf1 = session.time_series_for("users/ayende", ts_name1)
            tsf2 = session.time_series_for("users/gracjan", ts_name1)

            for i in range(360):
                tsf1.append_single(base_line + timedelta(seconds=i * 10), 6, tag1)

                if i % 2 == 0:
                    tsf2.append_single(base_line + timedelta(seconds=i * 10), 7, tag1)

            session.save_changes()

        with self.store.open_session() as session:
            users = session.load(
                ["users/ayende", "users/gracjan"],
                User,
                lambda i: i.include_time_series(ts_name1, base_line, base_line + timedelta(minutes=30)),
            )

            self.assertEqual(1, session.advanced.number_of_requests)

            self.assertEqual("Oren", users.get("users/ayende").name)
            self.assertEqual("Gracjan", users.get("users/gracjan").name)

            # should not go to server

            vals = session.time_series_for("users/ayende", ts_name1).get(base_line, base_line + timedelta(minutes=30))

            self.assertEqual(1, session.advanced.number_of_requests)

            self.assertEqual(181, len(vals))

            self.assertEqual(base_line, vals[0].timestamp)
            self.assertEqual(base_line + timedelta(minutes=30), vals[180].timestamp)

            # should not go to server

            vals = session.time_series_for("users/gracjan", ts_name1).get(base_line, base_line + timedelta(minutes=30))
            self.assertEqual(1, session.advanced.number_of_requests)

            self.assertEqual(91, len(vals))
            self.assertEqual(base_line, vals[0].timestamp)
            self.assertEqual(base_line + timedelta(minutes=30), vals[90].timestamp)

    def test_should_throw_on_including_time_series_with_none_range(self):
        with self.store.open_session() as session:
            self.assertRaisesWithMessage(
                session.load,
                ValueError,
                "Time range type cannot be set to NONE when time is specified.",
                "orders/1-A",
                Order,
                lambda i: i.include_documents("company").include_all_time_series_by_time(
                    TimeSeriesRangeType.NONE, TimeValue.of_minutes(-30)
                ),
            )
        with self.store.open_session() as session:
            self.assertRaisesWithMessage(
                session.load,
                ValueError,
                "Time range type cannot be set to NONE when time is specified.",
                "orders/1-A",
                Order,
                lambda i: i.include_documents("company").include_all_time_series_by_time(
                    TimeSeriesRangeType.NONE, TimeValue.ZERO()
                ),
            )

        with self.store.open_session() as session:
            self.assertRaisesWithMessage(
                session.load,
                ValueError,
                "Time range type cannot be set to NONE when count is specified.",
                "orders/1-A",
                Order,
                lambda i: i.include_documents("company").include_all_time_series_by_count(
                    TimeSeriesRangeType.NONE, 1024
                ),
            )

        with self.store.open_session() as session:
            self.assertRaisesWithMessage(
                session.load,
                ValueError,
                "Time range type cannot be set to NONE when time is specified.",
                "orders/1-A",
                Order,
                lambda i: i.include_documents("company").include_all_time_series_by_time(
                    TimeSeriesRangeType.NONE, TimeValue.of_minutes(30)
                ),
            )

        self.assertEqual(0, session.advanced.number_of_requests)

    def test_should_throw_on_including_time_series_with_last_range_zero_or_negative_time(self):
        with self.store.open_session() as session:
            self.assertRaisesWithMessage(
                session.load,
                ValueError,
                "Time range type cannot be set to LAST when time is negative or zero.",
                "orders/1-A",
                Order,
                lambda i: i.include_documents("company").include_all_time_series_by_time(
                    TimeSeriesRangeType.LAST, TimeValue.MIN_VALUE()
                ),
            )

            self.assertRaisesWithMessage(
                session.load,
                ValueError,
                "Time range type cannot be set to LAST when time is negative or zero.",
                "orders/1-A",
                Order,
                lambda i: i.include_documents("company").include_all_time_series_by_time(
                    TimeSeriesRangeType.LAST, TimeValue.ZERO()
                ),
            )

            self.assertEqual(0, session.advanced.number_of_requests)

    def test_should_throw_on_include_all_time_series_after_including_time_series(self):
        with self.store.open_session() as session:
            self.assertRaisesWithMessage(
                session.load,
                RuntimeError,
                "IncludeBuilder : Cannot use 'includeAllTimeSeries' after using 'includeTimeSeries' or 'includeAllTimeSeries'.",
                "orders/1-A",
                Order,
                lambda i: i.include_documents("company")
                .include_all_time_series_by_count(TimeSeriesRangeType.LAST, 11)
                .include_all_time_series_by_time(TimeSeriesRangeType.LAST, TimeValue.of_minutes(10)),
            )

            self.assertRaisesWithMessage(
                session.load,
                RuntimeError,
                "IncludeBuilder : Cannot use 'includeAllTimeSeries' after using 'includeTimeSeries' or 'includeAllTimeSeries'.",
                "orders/1-A",
                Order,
                lambda i: i.include_documents("company")
                .include_all_time_series_by_count(TimeSeriesRangeType.LAST, int_max)
                .include_all_time_series_by_time(TimeSeriesRangeType.LAST, TimeValue.of_minutes(11)),
            )

            self.assertRaisesWithMessage(
                session.load,
                RuntimeError,
                "IncludeBuilder : Cannot use 'includeAllTimeSeries' after using 'includeTimeSeries' or 'includeAllTimeSeries'.",
                "orders/1-A",
                Order,
                lambda i: i.include_documents("company")
                .include_time_series_by_range_type_and_count("heartrate", TimeSeriesRangeType.LAST, int_max)
                .include_all_time_series_by_time(TimeSeriesRangeType.LAST, TimeValue.of_minutes(10)),
            )

            self.assertRaisesWithMessage(
                session.load,
                RuntimeError,
                "IncludeBuilder : Cannot use 'includeAllTimeSeries' after using 'includeTimeSeries' or 'includeAllTimeSeries'.",
                "orders/1-A",
                Order,
                lambda i: i.include_documents("company")
                .include_time_series_by_range_type_and_count("heartrate", TimeSeriesRangeType.LAST, int_max)
                .include_all_time_series_by_count(TimeSeriesRangeType.LAST, 11),
            )

            self.assertRaisesWithMessage(
                session.load,
                RuntimeError,
                "IncludeBuilder : Cannot use 'includeAllTimeSeries' after using 'includeTimeSeries' or 'includeAllTimeSeries'.",
                "orders/1-A",
                Order,
                lambda i: i.include_documents("company")
                .include_time_series_by_range_type_and_count("heartrate", TimeSeriesRangeType.LAST, 11)
                .include_all_time_series_by_count(TimeSeriesRangeType.LAST, TimeValue.of_minutes(10)),
            )

            self.assertRaisesWithMessage(
                session.load,
                RuntimeError,
                "IncludeBuilder : Cannot use 'includeAllTimeSeries' after using 'includeTimeSeries' or 'includeAllTimeSeries'.",
                "orders/1-A",
                Order,
                lambda i: i.include_documents("company")
                .include_time_series_by_range_type_and_count("heartrate", TimeSeriesRangeType.LAST, 11)
                .include_all_time_series_by_count(TimeSeriesRangeType.LAST, TimeValue.of_minutes(11)),
            )

            self.assertEqual(0, session.advanced.number_of_requests)

    def test_should_throw_on_including_time_series_after_include_all_time_series(self):
        with self.store.open_session() as session:
            self.assertRaisesWithMessage(
                session.load,
                RuntimeError,
                "IncludeBuilder : Cannot use 'includeAllTimeSeries' "
                "after using 'includeTimeSeries' or 'includeAllTimeSeries'.",
                "orders/1-A",
                Order,
                lambda i: i.include_documents("company")
                .include_all_time_series_by_count(TimeSeriesRangeType.LAST, 11)
                .include_all_time_series_by_time(TimeSeriesRangeType.LAST, TimeValue.of_minutes(10)),
            )

            self.assertRaisesWithMessage(
                session.load,
                RuntimeError,
                "IncludeBuilder : Cannot use 'includeAllTimeSeries' "
                "after using 'includeTimeSeries' or 'includeAllTimeSeries'.",
                "orders/1-A",
                Order,
                lambda i: i.include_documents("company")
                .include_all_time_series_by_count(TimeSeriesRangeType.LAST, int_max)
                .include_all_time_series_by_count(TimeSeriesRangeType.LAST, 11),
            )
            self.assertRaisesWithMessage(
                session.load,
                RuntimeError,
                "IncludeBuilder: Cannot use 'includeTimeSeries' or 'includeAllTimeSeries' "
                "after using 'includeAllTimeSeries'.",
                "orders/1-A",
                Order,
                lambda i: i.include_documents("company")
                .include_all_time_series_by_time(TimeSeriesRangeType.LAST, TimeValue.of_minutes(10))
                .include_time_series_by_range_type_and_count("heartrate", TimeSeriesRangeType.LAST, int_max),
            )
            self.assertRaisesWithMessage(
                session.load,
                RuntimeError,
                "IncludeBuilder: Cannot use 'includeTimeSeries' or 'includeAllTimeSeries' "
                "after using 'includeAllTimeSeries'.",
                "orders/1-A",
                Order,
                lambda i: i.include_documents("company")
                .include_all_time_series_by_count(TimeSeriesRangeType.LAST, 11)
                .include_time_series_by_range_type_and_time(
                    "heartrate", TimeSeriesRangeType.LAST, TimeValue.MAX_VALUE()
                ),
            )
            self.assertRaisesWithMessage(
                session.load,
                RuntimeError,
                "IncludeBuilder: Cannot use 'includeTimeSeries' or 'includeAllTimeSeries' "
                "after using 'includeAllTimeSeries'.",
                "orders/1-A",
                Order,
                lambda i: i.include_documents("company")
                .include_all_time_series_by_time(TimeSeriesRangeType.LAST, TimeValue.of_minutes(10))
                .include_time_series_by_range_type_and_count("heartrate", TimeSeriesRangeType.LAST, 11),
            )
            self.assertRaisesWithMessage(
                session.load,
                RuntimeError,
                "IncludeBuilder: Cannot use 'includeTimeSeries' or 'includeAllTimeSeries' "
                "after using 'includeAllTimeSeries'.",
                "orders/1-A",
                Order,
                lambda i: i.include_documents("company")
                .include_all_time_series_by_count(TimeSeriesRangeType.LAST, 11)
                .include_time_series_by_range_type_and_time(
                    "heartrate", TimeSeriesRangeType.LAST, TimeValue.of_minutes(10)
                ),
            )

            self.assertRaisesWithMessage(
                session.load,
                RuntimeError,
                "IncludeBuilder : Cannot use 'includeAllTimeSeries' "
                "after using 'includeTimeSeries' or 'includeAllTimeSeries'.",
                "orders/1-A",
                Order,
                lambda i: i.include_documents("company")
                .include_all_time_series_by_count(TimeSeriesRangeType.LAST, 11)
                .include_all_time_series_by_time(TimeSeriesRangeType.LAST, TimeValue.of_minutes(10)),
            )

            self.assertRaisesWithMessage(
                session.load,
                RuntimeError,
                "IncludeBuilder : Cannot use 'includeAllTimeSeries' "
                "after using 'includeTimeSeries' or 'includeAllTimeSeries'.",
                "orders/1-A",
                Order,
                lambda i: i.include_documents("company")
                .include_all_time_series_by_time(TimeSeriesRangeType.LAST, TimeValue.MAX_VALUE())
                .include_all_time_series_by_count(TimeSeriesRangeType.LAST, 11),
            )

            self.assertRaisesWithMessage(
                session.load,
                RuntimeError,
                "IncludeBuilder: Cannot use 'includeTimeSeries' or 'includeAllTimeSeries' "
                "after using 'includeAllTimeSeries'.",
                "orders/1-A",
                Order,
                lambda i: i.include_documents("company")
                .include_all_time_series_by_time(TimeSeriesRangeType.LAST, TimeValue.of_minutes(10))
                .include_time_series_by_range_type_and_time(
                    "heartrate", TimeSeriesRangeType.LAST, TimeValue.MAX_VALUE()
                ),
            )

            self.assertRaisesWithMessage(
                session.load,
                RuntimeError,
                "IncludeBuilder: Cannot use 'includeTimeSeries' or 'includeAllTimeSeries' "
                "after using 'includeAllTimeSeries'.",
                "orders/1-A",
                Order,
                lambda i: i.include_documents("company")
                .include_all_time_series_by_count(TimeSeriesRangeType.LAST, 11)
                .include_time_series_by_range_type_and_time(
                    "heartrate", TimeSeriesRangeType.LAST, TimeValue.MAX_VALUE()
                ),
            )

            self.assertRaisesWithMessage(
                session.load,
                RuntimeError,
                "IncludeBuilder: Cannot use 'includeTimeSeries' or 'includeAllTimeSeries' "
                "after using 'includeAllTimeSeries'.",
                "orders/1-A",
                Order,
                lambda i: i.include_documents("company")
                .include_all_time_series_by_time(TimeSeriesRangeType.LAST, TimeValue.of_minutes(10))
                .include_time_series_by_range_type_and_count("heartrate", TimeSeriesRangeType.LAST, 11),
            )

            self.assertRaisesWithMessage(
                session.load,
                RuntimeError,
                "IncludeBuilder: Cannot use 'includeTimeSeries' or 'includeAllTimeSeries' "
                "after using 'includeAllTimeSeries'.",
                "orders/1-A",
                Order,
                lambda i: i.include_documents("company")
                .include_all_time_series_by_count(TimeSeriesRangeType.LAST, 11)
                .include_time_series_by_range_type_and_count("heartrate", TimeSeriesRangeType.LAST, 11),
            )

            self.assertEqual(0, session.advanced.number_of_requests)

    def test_should_throw_on_including_time_series_with_negative_count(self):
        with self.store.open_session() as session:
            self.assertRaisesWithMessage(
                session.load,
                ValueError,
                "Count have to be positive",
                "orders/1-A",
                Order,
                lambda i: i.include_documents("company").include_all_time_series_by_count(
                    TimeSeriesRangeType.LAST, -1024
                ),
            )

    def test_include_time_series_and_documents_and_counters(self):
        with self.store.open_session() as session:
            user = User()
            user.name = "Oren"
            user.works_at = "companies/1"
            session.store(user, "users/ayende")

            company = Company(name="HR")
            session.store(company, "companies/1")

            session.save_changes()

        with self.store.open_session() as session:
            tsf = session.time_series_for("users/ayende", "Heartrate")

            for i in range(360):
                tsf.append_single(base_line + timedelta(seconds=i * 10), 67, "watches/fitbit")

            session.counters_for("users/ayende").increment("likes", 100)
            session.counters_for("users/ayende").increment("dislikes", 5)
            session.save_changes()

        with self.store.open_session() as session:
            user = session.load(
                "users/ayende",
                User,
                lambda i: i.include_documents("works_at")
                .include_time_series("Heartrate", base_line, base_line + timedelta(minutes=30))
                .include_counter("likes")
                .include_counter("dislikes"),
            )

            self.assertEqual(1, session.advanced.number_of_requests)

            self.assertEqual("Oren", user.name)

            # should not go to server

            company = session.load(user.works_at, Company)
            self.assertEqual(1, session.advanced.number_of_requests)

            self.assertEqual("HR", company.name)

            # should not go to server
            vals = session.time_series_for("users/ayende", "Heartrate").get(
                base_line, base_line + timedelta(minutes=30)
            )
            self.assertEqual(1, session.advanced.number_of_requests)

            self.assertEqual(181, len(vals))

            self.assertEqual(base_line, vals[0].timestamp)
            self.assertEqual("watches/fitbit", vals[0].tag)
            self.assertEqual(67, vals[0].values[0])
            self.assertEqual(base_line + timedelta(minutes=30), vals[180].timestamp)

            # should not go to server
            counters = session.counters_for("users/ayende").get_all()

            self.assertEqual(1, session.advanced.number_of_requests)

            counter = counters.get("likes")
            self.assertEqual(100, counter)
            counter = counters.get("dislikes")
            self.assertEqual(5, counter)

from datetime import timedelta, datetime
from typing import List

from ravendb.documents.session.time_series import TypedTimeSeriesEntry, TimeSeriesEntry
from ravendb.infrastructure.entities import User
from ravendb.tests.jvm_migrated_tests.client_tests.time_series_tests.test_time_series_typed_session import (
    HeartRateMeasure,
    StockPrice,
)
from ravendb.tests.test_base import TestBase
from ravendb.tools.raven_test_helper import RavenTestHelper


class TestTypedBulkInsert(TestBase):
    def setUp(self):
        super().setUp()

    def test_can_store_and_read_multiple_timestamps(self):
        base_line = RavenTestHelper.utc_today()
        document_id = "users/ayende"

        with self.store.bulk_insert() as bulk_insert:
            user = User(name="Oren")
            bulk_insert.store_as(user, document_id)

            with bulk_insert.typed_time_series_for(HeartRateMeasure, document_id) as ts:
                ts.append_single(base_line + timedelta(minutes=1), HeartRateMeasure(59), "watches/fitbit")

        with self.store.bulk_insert() as bulk_insert:
            with bulk_insert.typed_time_series_for(HeartRateMeasure, document_id) as time_series_bulk_insert:
                time_series_bulk_insert.append_single(
                    base_line + timedelta(minutes=2), HeartRateMeasure(61), "watches/fitbit"
                )
                time_series_bulk_insert.append_single(
                    base_line + timedelta(minutes=3), HeartRateMeasure(62), "watches/apple-watch"
                )

        with self.store.open_session() as session:
            vals = session.typed_time_series_for(HeartRateMeasure, document_id).get()

            self.assertEqual(3, len(vals))

            self.assertEqual(59, vals[0].value.heart_rate)
            self.assertEqual("watches/fitbit", vals[0].tag)
            self.assertEqual(base_line + timedelta(minutes=1), vals[0].timestamp)

            self.assertEqual(61, vals[1].value.heart_rate)
            self.assertEqual("watches/fitbit", vals[1].tag)
            self.assertEqual(base_line + timedelta(minutes=2), vals[1].timestamp)

            self.assertEqual(62, vals[2].value.heart_rate)
            self.assertEqual("watches/apple-watch", vals[2].tag)
            self.assertEqual(base_line + timedelta(minutes=3), vals[2].timestamp)

    def test_using_different_tags(self):
        base_line = RavenTestHelper.utc_today()
        document_id = "users/ayende"

        with self.store.bulk_insert() as bulk_insert:
            user = User(name="Oren")
            bulk_insert.store_as(user, document_id)

            with bulk_insert.typed_time_series_for(HeartRateMeasure, document_id, "heartrate") as ts:
                ts.append_single(base_line + timedelta(minutes=1), HeartRateMeasure(59), "watches/fitbit")
                ts.append_single(base_line + timedelta(minutes=2), HeartRateMeasure(70), "watches/apple-watch")

        with self.store.open_session() as session:
            vals = session.time_series_for(document_id, "heartrate").get()

            self.assertEqual(2, len(vals))

            self.assertEqual([59], vals[0].values)
            self.assertEqual("watches/fitbit", vals[0].tag)
            self.assertEqual(base_line + timedelta(minutes=1), vals[0].timestamp)

            self.assertEqual([70], vals[1].values)
            self.assertEqual("watches/apple-watch", vals[1].tag)
            self.assertEqual(base_line + timedelta(minutes=2), vals[1].timestamp)

    def test_can_create_simple_time_series(self):
        base_line = RavenTestHelper.utc_today()
        document_id = "users/ayende"

        with self.store.bulk_insert() as bulk_insert:
            user = User(name="Oren")
            bulk_insert.store_as(user, document_id)

            with bulk_insert.typed_time_series_for(
                HeartRateMeasure, document_id, "heartrate"
            ) as time_series_bulk_insert:
                measure = TypedTimeSeriesEntry()
                measure.timestamp = base_line
                measure2 = HeartRateMeasure(59)
                measure.value = measure2
                measure.tag = "watches/fitbit"

                time_series_bulk_insert.append_entry(measure)

        with self.store.open_session() as session:
            val = session.typed_time_series_for(HeartRateMeasure, document_id, "heartrate").get()[0]

            self.assertEqual(59, val.value.heart_rate)
            self.assertEqual("watches/fitbit", val.tag)
            self.assertEqual(base_line, val.timestamp)

    def test_can_store_and_read_multiple_timeseries_for_different_documents(self):
        base_line = RavenTestHelper.utc_today()
        document_id1 = "users/ayende"
        document_id2 = "users/grisha"

        with self.store.bulk_insert() as bulk_insert:
            user = User()
            user.name = "Oren"
            bulk_insert.store_as(user, document_id1)
            with bulk_insert.typed_time_series_for(
                HeartRateMeasure, document_id1, "heartrate"
            ) as time_series_bulk_insert:
                time_series_bulk_insert.append_single(
                    base_line + timedelta(minutes=1), HeartRateMeasure(59), "watches/fitbit"
                )

            user2 = User(name="Grisha")
            bulk_insert.store_as(user2, document_id2)

            with bulk_insert.typed_time_series_for(
                HeartRateMeasure, document_id2, "heartrate"
            ) as time_series_bulk_insert:
                time_series_bulk_insert.append_single(
                    base_line + timedelta(minutes=1), HeartRateMeasure(59), "watches/fitbit"
                )

        with self.store.bulk_insert() as bulk_insert:
            with bulk_insert.typed_time_series_for(
                HeartRateMeasure, document_id1, "heartrate"
            ) as time_series_bulk_insert:
                measure = TypedTimeSeriesEntry()
                measure.timestamp = base_line + timedelta(minutes=2)
                measure.tag = "watches/fitbit"
                measure.value = HeartRateMeasure(61)

                time_series_bulk_insert.append_entry(measure)

            with bulk_insert.typed_time_series_for(
                HeartRateMeasure, document_id2, "heartrate"
            ) as time_series_bulk_insert:
                measure = TypedTimeSeriesEntry()
                measure.timestamp = base_line + timedelta(minutes=2)
                measure.tag = "watches/fitbit"
                measure.value = HeartRateMeasure(61)

                time_series_bulk_insert.append_entry(measure)

            with bulk_insert.typed_time_series_for(
                HeartRateMeasure, document_id1, "heartrate"
            ) as time_series_bulk_insert:
                measure = TypedTimeSeriesEntry()
                measure.timestamp = base_line + timedelta(minutes=3)
                measure.tag = "watches/apple-watch"
                measure.value = HeartRateMeasure(62)

                time_series_bulk_insert.append_entry(measure)

            with bulk_insert.typed_time_series_for(
                HeartRateMeasure, document_id2, "heartrate"
            ) as time_series_bulk_insert:
                measure = TypedTimeSeriesEntry()
                measure.timestamp = base_line + timedelta(minutes=3)
                measure.tag = "watches/apple-watch"
                measure.value = HeartRateMeasure(62)

                time_series_bulk_insert.append_entry(measure)

        with self.store.open_session() as session:
            vals = session.typed_time_series_for(HeartRateMeasure, document_id1, "heartrate").get()

            def __validate_values(base_line: datetime, vals: List[TypedTimeSeriesEntry[HeartRateMeasure]]) -> None:
                self.assertEqual(3, len(vals))

                self.assertEqual(59, vals[0].value.heart_rate)
                self.assertEqual("watches/fitbit", vals[0].tag)
                self.assertEqual(base_line + timedelta(minutes=1), vals[0].timestamp)

                self.assertEqual(61, vals[1].value.heart_rate)
                self.assertEqual("watches/fitbit", vals[1].tag)
                self.assertEqual(base_line + timedelta(minutes=2), vals[1].timestamp)

                self.assertEqual(62, vals[2].value.heart_rate)
                self.assertEqual("watches/apple-watch", vals[2].tag)
                self.assertEqual(base_line + timedelta(minutes=3), vals[2].timestamp)

            __validate_values(base_line, vals)

            vals = session.typed_time_series_for(HeartRateMeasure, document_id2, "heartrate").get()
            __validate_values(base_line, vals)

    def test_can_get_time_series_names(self):
        document_id_1 = "users/karmel"
        document_id_2 = "users/ayende"

        with self.store.bulk_insert() as bulk_insert:
            bulk_insert.store_as(User(), document_id_1)
            with bulk_insert.typed_time_series_for(StockPrice, document_id_1, "nasdaq2") as ts:
                stock_price = StockPrice()
                stock_price.open = 7547.31
                stock_price.close = 7123.5
                ts.append_single(datetime.utcnow(), stock_price, "web")

        with self.store.bulk_insert() as bulk_insert:
            with bulk_insert.typed_time_series_for(HeartRateMeasure, document_id_1, "heartrate2") as ts:
                heart_rate_measure = HeartRateMeasure(76)
                ts.append_single(datetime.utcnow(), heart_rate_measure, "watches/apple")

        with self.store.bulk_insert() as bulk_insert:
            bulk_insert.store_as(User(), document_id_2)
            with bulk_insert.typed_time_series_for(StockPrice, document_id_2, "nasdaq") as ts:
                stock_price = StockPrice()
                stock_price.open = 7547.31
                stock_price.close = 7123.5
                ts.append_single(datetime.utcnow(), stock_price, "web")

        with self.store.bulk_insert() as bulk_insert:
            with bulk_insert.typed_time_series_for(HeartRateMeasure, document_id_2, "heartrate") as ts:
                ts.append_single(datetime.utcnow(), HeartRateMeasure(58), "fitbit")

        with self.store.open_session() as session:
            user = session.load(document_id_2, User)
            ts_names = session.advanced.get_time_series_for(user)
            self.assertEqual(2, len(ts_names))

            # should be sorted
            self.assertEqual("heartrate", ts_names[0])
            self.assertEqual("nasdaq", ts_names[1])

    def test_can_create_time_series_without_passing_name(self):
        base_line = RavenTestHelper.utc_today()
        document_id = "users/ayende"

        with self.store.bulk_insert() as bulk_insert:
            user = User(name="Oren")
            bulk_insert.store_as(user, document_id)

            with bulk_insert.typed_time_series_for(StockPrice, document_id) as time_series_bulk_insert:
                measure = TypedTimeSeriesEntry()
                measure.timestamp = base_line
                stock_price = StockPrice()
                stock_price.close = 1
                stock_price.open = 2
                stock_price.high = 3
                stock_price.low = 4
                stock_price.volume = 55
                measure.value = stock_price
                measure.tag = "tag"

                time_series_bulk_insert.append_entry(measure)

        with self.store.open_session() as session:
            val = session.typed_time_series_for(StockPrice, document_id).get()[0]
            self.assertEqual(1, val.value.close)
            self.assertEqual(2, val.value.open)
            self.assertEqual(3, val.value.high)
            self.assertEqual(4, val.value.low)
            self.assertEqual(55, val.value.volume)

            self.assertEqual("tag", val.tag)
            self.assertEqual(base_line, val.timestamp)

        with self.store.open_session() as session:
            doc = session.load(document_id, User)
            names = session.advanced.get_time_series_for(doc)

            self.assertEqual(1, len(names))
            self.assertEqual("StockPrices", names[0])

    def test_can_create_simple_time_series_2(self):
        base_line = RavenTestHelper.utc_this_month()
        document_id = "users/ayende"

        with self.store.bulk_insert() as bulk_insert:
            user = User(name="Oren")
            bulk_insert.store_as(user, document_id)

            measure = TypedTimeSeriesEntry()
            measure.timestamp = base_line + timedelta(minutes=1)
            measure.value = HeartRateMeasure(59)
            measure.tag = "watches/fitbit"

            with bulk_insert.typed_time_series_for(
                HeartRateMeasure, document_id, "heartrate"
            ) as time_series_bulk_insert:
                time_series_bulk_insert.append_single(
                    base_line + timedelta(minutes=1), HeartRateMeasure(59), "watches/fitbit"
                )
                time_series_bulk_insert.append_single(
                    base_line + timedelta(minutes=2), HeartRateMeasure(60), "watches/fitbit"
                )
                time_series_bulk_insert.append_single(
                    base_line + timedelta(minutes=2), HeartRateMeasure(61), "watches/fitbit"
                )

        with self.store.open_session() as session:
            val = session.typed_time_series_for(HeartRateMeasure, document_id, "heartrate").get()
            self.assertEqual(2, len(val))

    def test_can_delete_timestamp(self):
        base_line = RavenTestHelper.utc_today()
        document_id = "users/ayende"

        with self.store.bulk_insert() as bulk_insert:
            user = User(name="Oren")
            bulk_insert.store(user)
            bulk_insert.store_as(user, document_id)

            with bulk_insert.typed_time_series_for(
                HeartRateMeasure, document_id, "heartrate"
            ) as time_series_bulk_insert:
                time_series_bulk_insert.append_single(
                    base_line + timedelta(minutes=1), HeartRateMeasure(59), "watches/fitbit"
                )
                time_series_bulk_insert.append_single(
                    base_line + timedelta(minutes=2), HeartRateMeasure(69), "watches/fitbit"
                )
                time_series_bulk_insert.append_single(
                    base_line + timedelta(minutes=3), HeartRateMeasure(79), "watches/fitbit"
                )

        with self.store.open_session() as session:
            user = User(name="Oren")
            session.store(user, document_id)
            session.time_series_for(document_id, "heartrate").delete_at(base_line + timedelta(minutes=2))
            session.save_changes()

        with self.store.open_session() as session:
            vals = session.time_series_for(document_id, "heartrate").get()

            self.assertEqual(2, len(vals))
            self.assertEqual([59], vals[0].values)
            self.assertEqual("watches/fitbit", vals[0].tag)
            self.assertEqual(base_line + timedelta(minutes=1), vals[0].timestamp)

            self.assertEqual([79], vals[1].values)
            self.assertEqual("watches/fitbit", vals[1].tag)
            self.assertEqual(base_line + timedelta(minutes=3), vals[1].timestamp)

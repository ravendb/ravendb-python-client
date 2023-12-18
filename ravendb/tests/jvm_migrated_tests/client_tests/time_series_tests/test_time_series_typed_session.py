import time
import unittest
from datetime import datetime, timedelta
from typing import Dict, Tuple, Optional

from ravendb.documents.operations.time_series import (
    TimeSeriesPolicy,
    TimeSeriesCollectionConfiguration,
    TimeSeriesConfiguration,
    ConfigureTimeSeriesOperation,
    RawTimeSeriesPolicy,
)
from ravendb.documents.queries.time_series import TimeSeriesRawResult, TimeSeriesAggregationResult
from ravendb.documents.session.time_series import (
    ITimeSeriesValuesBindable,
    TypedTimeSeriesEntry,
    TypedTimeSeriesRollupEntry,
)
from ravendb.infrastructure.entities import User
from ravendb.infrastructure.orders import Company
from ravendb.primitives.time_series import TimeValue
from ravendb.tests.test_base import TestBase
from ravendb.tools.raven_test_helper import RavenTestHelper

document_id = "users/gracjan"
company_id = "companies/1-A"
order_id = "orders/1-A"
base_line = datetime(2023, 8, 20, 21, 30)
ts_name1 = "Heartrate"
ts_name2 = "Speedrate"
tag1 = "watches/fitbit"
tag2 = "watches/apple"
tag3 = "watches/sony"


class HeartRateMeasure(ITimeSeriesValuesBindable):
    def __init__(self, value: float):
        self.heart_rate = value

    def get_time_series_mapping(self) -> Dict[int, Tuple[str, Optional[str]]]:
        return {0: ("heart_rate", None)}


class BigMeasure(ITimeSeriesValuesBindable):
    def __init__(self, m1, m2, m3, m4, m5, m6):
        self.measure1 = m1
        self.measure2 = m2
        self.measure3 = m3
        self.measure4 = m4
        self.measure5 = m5
        self.measure6 = m6

    def get_time_series_mapping(self) -> Dict[int, Tuple[str, Optional[str]]]:
        return {
            0: ("measure1", None),
            1: ("measure2", None),
            2: ("measure3", None),
            3: ("measure4", None),
            4: ("measure5", None),
            5: ("measure6", None),
        }


class StockPrice(ITimeSeriesValuesBindable):
    def __init__(
        self,
        open: Optional[float] = None,
        close: Optional[float] = None,
        high: Optional[float] = None,
        low: Optional[float] = None,
        volume: Optional[float] = None,
    ):
        self.open = open
        self.close = close
        self.high = high
        self.low = low
        self.volume = volume

    def get_time_series_mapping(self) -> Dict[int, Tuple[str, Optional[str]]]:
        return {
            0: ("open", None),
            1: ("close", None),
            2: ("high", None),
            3: ("low", None),
            4: ("volume", None),
        }


class StockPriceWithBadAttributes(ITimeSeriesValuesBindable):
    def __init__(
        self,
        open: Optional[float] = None,
        close: Optional[float] = None,
        high: Optional[float] = None,
        low: Optional[float] = None,
        volume: Optional[float] = None,
    ):
        self.open = open
        self.close = close
        self.high = high
        self.low = low
        self.volume = volume

    def get_time_series_mapping(self) -> Dict[int, Tuple[str, Optional[str]]]:
        return {
            1: ("open", None),
            2: ("close", None),
            3: ("high", None),
            4: ("low", None),
            5: ("volume", None),
        }


class TestTimeSeriesTypedSession(TestBase):
    def setUp(self):
        super(TestTimeSeriesTypedSession, self).setUp()

    def test_can_request_non_existing_time_series_range(self):
        with self.store.open_session() as session:
            session.store(User(), document_id)
            tsf = session.time_series_for(document_id, ts_name1)
            tsf.append_single(base_line, 58, tag1)
            tsf.append_single(base_line + timedelta(minutes=10), 60, tag1)
            session.save_changes()

        with self.store.open_session() as session:
            vals = session.typed_time_series_for(HeartRateMeasure, document_id).get(
                base_line - timedelta(minutes=10), base_line - timedelta(minutes=5)
            )
            self.assertIsNone(vals)

            vals = session.typed_time_series_for(HeartRateMeasure, document_id).get(
                base_line + timedelta(minutes=5), base_line + timedelta(minutes=9)
            )
            self.assertIsNone(vals)

    def test_can_create_simple_time_series(self):
        with self.store.open_session() as session:
            session.store(User(), document_id)
            heart_rate_measure = HeartRateMeasure(59)
            ts = session.typed_time_series_for(HeartRateMeasure, document_id)
            ts.append(base_line, heart_rate_measure, tag1)
            session.save_changes()

        with self.store.open_session() as session:
            val = session.typed_time_series_for(HeartRateMeasure, document_id).get()[0]
            self.assertEqual(59, val.value.heart_rate)
            self.assertEqual(tag1, val.tag)
            self.assertEqual(base_line, val.timestamp)

    def test_can_create_simple_time_series_async(self):
        with self.store.open_session() as session:
            session.store(User(), document_id)
            heart_rate_measure = HeartRateMeasure(59)

            measure = TypedTimeSeriesEntry(base_line + timedelta(minutes=1), tag1)
            measure.value = heart_rate_measure

            ts = session.typed_time_series_for(HeartRateMeasure, document_id)
            ts.append_entry(measure)

            session.save_changes()

        with self.store.open_session() as session:
            val = session.typed_time_series_for(HeartRateMeasure, document_id).get()[0]
            self.assertEqual(59, val.value.heart_rate)
            self.assertEqual(tag1, val.tag)
            self.assertEqual(base_line + timedelta(minutes=1), val.timestamp)

    def test_using_different_number_of_values_large_to_small(self):
        with self.store.open_session() as session:
            session.store(User(name="Gracjan"), document_id)
            big = session.typed_time_series_for(BigMeasure, document_id)
            for i in range(5):
                big.append(base_line + timedelta(seconds=i * 3), BigMeasure(i, i, i, i, i, i), tag1)

            session.save_changes()

        with self.store.open_session() as session:
            big = session.time_series_for(document_id, "BigMeasures")

            for i in range(5, 10):
                big.append_single(base_line + timedelta(hours=12, seconds=i * 3), i, tag1)

            session.save_changes()

        with self.store.open_session() as session:
            big = session.typed_time_series_for(BigMeasure, document_id).get()
            for i in range(5):
                m = big[i].value
                self.assertEqual(i, m.measure1)
                self.assertEqual(i, m.measure2)
                self.assertEqual(i, m.measure3)
                self.assertEqual(i, m.measure4)
                self.assertEqual(i, m.measure5)
                self.assertEqual(i, m.measure6)

            for i in range(5, 10):
                m = big[i].value
                self.assertEqual(i, m.measure1)
                self.assertIsNone(m.measure2)
                self.assertIsNone(m.measure3)
                self.assertIsNone(m.measure4)
                self.assertIsNone(m.measure5)
                self.assertIsNone(m.measure6)

    def test_can_get_time_series_names(self):
        with self.store.open_session() as session:
            session.store(User(), document_id)
            heart_rate_measure = HeartRateMeasure(66)
            session.typed_time_series_for(HeartRateMeasure, document_id).append(
                base_line, heart_rate_measure, "MyHeart"
            )

            stock_price = StockPrice(66, 55, 113.4, 52.4, 15472)
            session.typed_time_series_for(StockPrice, document_id).append(base_line, stock_price)

            session.save_changes()

        with self.store.open_session() as session:
            user = session.load(document_id, User)
            ts_names = session.advanced.get_time_series_for(user)
            self.assertEqual(2, len(ts_names))

            # should be sorted
            self.assertEqual("HeartRateMeasures", ts_names[0])
            self.assertEqual("StockPrices", ts_names[1])

            heart_rate_measures = session.typed_time_series_for_entity(HeartRateMeasure, user).get()[0]
            self.assertEqual(66, heart_rate_measures.value.heart_rate)

            stock_price_entry = session.typed_time_series_for_entity(StockPrice, user).get()[0]
            self.assertEqual(66, stock_price_entry.value.open)
            self.assertEqual(55, stock_price_entry.value.close)
            self.assertEqual(113.4, stock_price_entry.value.high)
            self.assertEqual(52.4, stock_price_entry.value.low)
            self.assertEqual(15472, stock_price_entry.value.volume)

    def test_can_create_simple_time_series_2(self):
        with self.store.open_session() as session:
            session.store(User(), document_id)
            tsf = session.time_series_for(document_id, "HeartRateMeasures")
            tsf.append_single(base_line + timedelta(minutes=1), 59, tag1)
            tsf.append_single(base_line + timedelta(minutes=2), 60, tag1)
            tsf.append_single(base_line + timedelta(minutes=2), 61, tag1)

            session.save_changes()

        with self.store.open_session() as session:
            val = session.typed_time_series_for(HeartRateMeasure, document_id).get()
            self.assertEqual(2, len(val))

            self.assertEqual(59, val[0].value.heart_rate)
            self.assertEqual(61, val[1].value.heart_rate)

    @unittest.skip("Insufficient license permissions. Skipping on CI/CD.")
    def test_can_execute_simple_rollup(self):
        p1 = TimeSeriesPolicy("BySecond", TimeValue.of_seconds(1))
        p2 = TimeSeriesPolicy("By2Seconds", TimeValue.of_seconds(2))
        p3 = TimeSeriesPolicy("By4Seconds", TimeValue.of_seconds(4))

        collection_config = TimeSeriesCollectionConfiguration()
        collection_config.policies = [p1, p2, p3]

        config = TimeSeriesConfiguration()
        config.collections = {"Users": collection_config}

        config.policy_check_frequency = timedelta(seconds=1)

        self.store.maintenance.send(ConfigureTimeSeriesOperation(config))

        with self.store.open_session() as session:
            session.store(User(name="Gracjan"), document_id)
            for i in range(100):
                session.time_series_for(document_id, ts_name1).append_single(
                    base_line + timedelta(milliseconds=400 * i), 29.0 * i, tag1
                )

            session.save_changes()

        # wait for rollups to run
        time.sleep(1.2)

        with self.store.open_session() as session:
            ts = session.time_series_for(document_id, ts_name1).get()
            ts_millis = ts[-1].timestamp - ts[0].timestamp

            ts1 = session.time_series_for(document_id, p1.get_time_series_name(ts_name1)).get()
            ts1_millis = ts1[-1].timestamp - ts1[0].timestamp

            self.assertEqual(ts_millis - timedelta(milliseconds=600), ts1_millis)

            ts2 = session.time_series_for(document_id, p2.get_time_series_name(ts_name1)).get()
            self.assertEqual(len(ts1) // 2, len(ts2))

            ts3 = session.time_series_for(document_id, p3.get_time_series_name(ts_name1)).get()
            self.assertEqual(len(ts1) // 4, len(ts3))

    @unittest.skip("Insufficient license permissions. Skipping on CI/CD.")
    def test_can_work_with_rollup_time_series_2(self):
        raw_hours = 24
        raw = RawTimeSeriesPolicy(TimeValue.of_hours(raw_hours))

        p1 = TimeSeriesPolicy("By6Hours", TimeValue.of_hours(6), TimeValue.of_hours(raw_hours * 4))
        p2 = TimeSeriesPolicy("By1Day", TimeValue.of_days(1), TimeValue.of_hours(raw_hours * 5))
        p3 = TimeSeriesPolicy("By30Minutes", TimeValue.of_minutes(30), TimeValue.of_hours(raw_hours * 2))
        p4 = TimeSeriesPolicy("By1Hour", TimeValue.of_hours(1), TimeValue.of_hours(raw_hours * 3))

        time_series_collection_configuration = TimeSeriesCollectionConfiguration()
        time_series_collection_configuration.raw_policy = raw
        time_series_collection_configuration.policies = [p1, p2, p3, p4]

        config = TimeSeriesConfiguration()
        config.collections = {"users": time_series_collection_configuration}
        config.policy_check_frequency = timedelta(milliseconds=100)

        self.store.maintenance.send(ConfigureTimeSeriesOperation(config))
        self.store.time_series.register_type(User, StockPrice)

        total = TimeValue.of_days(12).value // 60
        base_line = RavenTestHelper.utc_today() - timedelta(days=12)

        with self.store.open_session() as session:
            session.store(User(name="Karmel"), "users/karmel")

            ts = session.typed_time_series_for(StockPrice, "users/karmel")
            for i in range(total + 1):
                open = i
                close = i + 100_000
                high = i + 200_000
                low = i + 300_000
                volume = i + 400_000
                ts.append(
                    base_line + timedelta(minutes=i), StockPrice(open, close, high, low, volume), "watches/fitbit"
                )

            session.save_changes()

        time.sleep(1.5)  # wait for rollups

        with self.store.open_session() as session:
            ts1 = session.time_series_rollup_for(StockPrice, "users/karmel", p1.name)
            r = ts1.get()[0]
            self.assertIsNotNone(r.first)
            self.assertIsNotNone(r.last)
            self.assertIsNotNone(r.min)
            self.assertIsNotNone(r.max)
            self.assertIsNotNone(r.count)
            self.assertIsNotNone(r.average)

    @unittest.skip("Insufficient license permissions. Skipping on CI/CD.")
    def test_can_work_with_rollup_time_series(self):
        raw_hours = 24
        raw = RawTimeSeriesPolicy(TimeValue.of_hours(raw_hours))

        p1 = TimeSeriesPolicy("By6Hours", TimeValue.of_hours(6), TimeValue.of_hours(raw_hours * 4))
        p2 = TimeSeriesPolicy("By1Day", TimeValue.of_days(1), TimeValue.of_hours(raw_hours * 5))
        p3 = TimeSeriesPolicy("By30Minutes", TimeValue.of_minutes(30), TimeValue.of_hours(raw_hours * 2))
        p4 = TimeSeriesPolicy("By1Hour", TimeValue.of_hours(1), TimeValue.of_hours(raw_hours * 3))

        users_config = TimeSeriesCollectionConfiguration()
        users_config.raw_policy = raw
        users_config.policies = [p1, p2, p3, p4]

        config = TimeSeriesConfiguration()
        config.collections = {"Users": users_config}
        config.policy_check_frequency = timedelta(milliseconds=100)

        self.store.maintenance.send(ConfigureTimeSeriesOperation(config))
        self.store.time_series.register_type(User, StockPrice)

        # please notice we don't modify server time here!

        now = datetime.utcnow()
        base_line = RavenTestHelper.utc_today() - timedelta(days=12)

        total = TimeValue.of_days(12).value // 60

        with self.store.open_session() as session:
            session.store(User(name="Karmel"), "users/karmel")

            ts = session.typed_time_series_for(StockPrice, "users/karmel")
            for i in range(total + 1):
                open = i
                close = i + 100_000
                high = i + 200_000
                low = i + 300_000
                volume = i + 400_000
                ts.append(
                    base_line + timedelta(minutes=i), StockPrice(open, close, high, low, volume), "watches/fitbit"
                )

            session.save_changes()

        time.sleep(1.5)  # wait for rollups

        with self.store.open_session() as session:
            query = (
                session.advanced.raw_query(
                    "declare timeseries out()\n"
                    "{\n"
                    "    from StockPrices\n"
                    "    between $start and $end\n"
                    "}\n"
                    "from Users as u\n"
                    "select out()",
                    TimeSeriesRawResult,
                )
                .add_parameter("start", base_line - timedelta(days=1))
                .add_parameter("end", now + timedelta(days=1))
            )
            result_raw = query.single()
            result = result_raw.as_typed_result(StockPrice)

            self.assertGreater(len(result.results), 0)

            for res in result.results:
                if res.is_rollup:
                    self.assertGreater(len(res.values), 0)
                    self.assertGreater(len(res.value.low), 0)
                    self.assertGreater(len(res.value.high), 0)
                else:
                    self.assertEqual(5, len(res.values))

        now = datetime.utcnow()

        with self.store.open_session() as session:
            ts = session.time_series_rollup_for(StockPrice, "users/karmel", p1.name)
            a = TypedTimeSeriesRollupEntry(StockPrice, datetime.utcnow())
            a.max.close = 1
            ts.append(a)
            session.save_changes()

        with self.store.open_session() as session:
            ts = session.time_series_rollup_for(StockPrice, "users/karmel", p1.name)

            res = ts.get(now - timedelta(milliseconds=1), now + timedelta(days=1))
            self.assertEqual(1, len(res))
            self.assertEqual(1, res[0].max.close)

    def test_mapping_needs_to_contain_consecutive_values_starting_from_zero(self):
        self.assertRaisesWithMessage(
            self.store.time_series.register_type,
            RuntimeError,
            "The mapping of 'StockPriceWithBadAttributes' must contain consecutive values starting from 0.",
            Company,
            StockPriceWithBadAttributes,
        )

    def test_can_query_time_series_aggregation_declare_syntax_all_docs_query(self):
        base_line = RavenTestHelper.utc_today()
        with self.store.open_session() as session:
            session.store(User(), document_id)
            tsf = session.typed_time_series_for(HeartRateMeasure, document_id)
            m = HeartRateMeasure(59)
            tsf.append(base_line + timedelta(minutes=61), m, tag1)
            m.heart_rate = 79
            tsf.append(base_line + timedelta(minutes=62), m, tag1)
            m.heart_rate = 69
            tsf.append(base_line + timedelta(minutes=63), m, tag1)
            session.save_changes()

        with self.store.open_session() as session:
            query = (
                session.advanced.raw_query(
                    "declare timeseries out(u)\n"
                    "    {\n"
                    "        from u.HeartRateMeasures between $start and $end\n"
                    "        group by 1h\n"
                    "        select min(), max(), first(), last()\n"
                    "    }\n"
                    "    from @all_docs as u\n"
                    "    where id() == 'users/gracjan'\n"
                    "    select out(u)",
                    TimeSeriesAggregationResult,
                )
                .add_parameter("start", base_line)
                .add_parameter("end", base_line + timedelta(days=1))
            )

            agg = query.first().as_typed_result(HeartRateMeasure)

            self.assertEqual(3, agg.count)
            self.assertEqual(1, len(agg.results))

            val = agg.results[0]
            self.assertEqual(59, val.first.heart_rate)
            self.assertEqual(59, val.min.heart_rate)

            self.assertEqual(69, val.last.heart_rate)
            self.assertEqual(79, val.max.heart_rate)

            self.assertEqual(base_line + timedelta(minutes=60), val.from_date)
            self.assertEqual(base_line + timedelta(minutes=120), val.to_date)

    def test_can_query_time_series_aggregation_no_select_or_group_by(self):
        base_line = RavenTestHelper.utc_today()
        with self.store.open_session() as session:
            for i in range(1, 4):
                id = f"people/{i}"
                session.store(User(name="Oren", age=i * 30), id)
                tsf = session.typed_time_series_for(HeartRateMeasure, id)
                tsf.append(base_line + timedelta(minutes=61), HeartRateMeasure(59), tag1)
                tsf.append(base_line + timedelta(minutes=62), HeartRateMeasure(79), tag1)
                tsf.append(base_line + timedelta(minutes=63), HeartRateMeasure(69), tag2)
                tsf.append(base_line + timedelta(minutes=61, days=31), HeartRateMeasure(159), tag1)
                tsf.append(base_line + timedelta(minutes=62, days=31), HeartRateMeasure(179), tag2)
                tsf.append(base_line + timedelta(minutes=63, days=31), HeartRateMeasure(169), tag1)

            session.save_changes()

        with self.store.open_session() as session:
            query = (
                session.advanced.raw_query(
                    "declare timeseries out(x)\n"
                    "{\n"
                    "    from x.HeartRateMeasures between $start and $end\n"
                    "}\n"
                    "from Users as doc\n"
                    "where doc.age > 49\n"
                    "select out(doc)",
                    TimeSeriesRawResult,
                )
                .add_parameter("start", base_line)
                .add_parameter("end", base_line + timedelta(days=62))
            )

            result = list(query)

            self.assertEqual(2, len(result))

            for i in range(2):
                agg_raw = result[i]
                agg = agg_raw.as_typed_result(HeartRateMeasure)

                self.assertEqual(6, len(agg.results))

                val = agg.results[0]

                self.assertEqual(1, len(val.values))
                self.assertEqual(59, val.value.heart_rate)
                self.assertEqual(tag1, val.tag)
                self.assertEqual(base_line + timedelta(minutes=61), val.timestamp)

                val = agg.results[1]
                self.assertEqual(1, len(val.values))
                self.assertEqual(79, val.value.heart_rate)
                self.assertEqual(tag1, val.tag)
                self.assertEqual(base_line + timedelta(minutes=62), val.timestamp)

                val = agg.results[2]
                self.assertEqual(1, len(val.values))
                self.assertEqual(69, val.value.heart_rate)
                self.assertEqual(tag2, val.tag)
                self.assertEqual(base_line + timedelta(minutes=63), val.timestamp)

                val = agg.results[3]
                self.assertEqual(1, len(val.values))
                self.assertEqual(159, val.value.heart_rate)
                self.assertEqual(tag1, val.tag)
                self.assertEqual(base_line + timedelta(minutes=61, days=31), val.timestamp)

                val = agg.results[4]
                self.assertEqual(1, len(val.values))
                self.assertEqual(179, val.value.heart_rate)
                self.assertEqual(tag2, val.tag)
                self.assertEqual(base_line + timedelta(minutes=62, days=31), val.timestamp)

                val = agg.results[5]
                self.assertEqual(1, len(val.values))
                self.assertEqual(169, val.value.heart_rate)
                self.assertEqual(tag1, val.tag)
                self.assertEqual(base_line + timedelta(minutes=63, days=31), val.timestamp)

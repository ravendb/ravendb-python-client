import time
from datetime import datetime, timedelta
from typing import Dict, Tuple, Optional

from ravendb.documents.operations.time_series import (
    TimeSeriesPolicy,
    TimeSeriesCollectionConfiguration,
    TimeSeriesConfiguration,
    ConfigureTimeSeriesOperation,
)
from ravendb.documents.session.time_series import ITimeSeriesValuesBindable, TypedTimeSeriesEntry
from ravendb.infrastructure.entities import User
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

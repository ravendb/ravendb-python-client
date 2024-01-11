from __future__ import annotations

import time
import unittest
from datetime import datetime, timedelta
from typing import Dict, Tuple, Optional

from ravendb.documents.operations.time_series import (
    RawTimeSeriesPolicy,
    TimeSeriesPolicy,
    TimeSeriesCollectionConfiguration,
    TimeSeriesConfiguration,
    ConfigureTimeSeriesOperation,
)
from ravendb.documents.session.time_series import ITimeSeriesValuesBindable, TimeSeriesRangeType
from ravendb.infrastructure.entities import User
from ravendb.primitives.time_series import TimeValue
from ravendb.tests.jvm_migrated_tests.client_tests.time_series_tests.test_time_series_typed_session import StockPrice
from ravendb.tests.test_base import TestBase
from ravendb.tools.raven_test_helper import RavenTestHelper


class HeartRateMeasure(ITimeSeriesValuesBindable):
    def __init__(self, value: float):
        self.heart_rate = value

    def get_time_series_mapping(self) -> Dict[int, Tuple[str, Optional[str]]]:
        return {0: ("heart_rate", None)}


class TestRavenDB16060(TestBase):
    def setUp(self):
        super().setUp()

    def test_can_serve_time_series_from_cache_typed(self):
        base_line = datetime(2023, 8, 20, 21, 30)
        doc_id = "users/0x901507"

        with self.store.open_session() as session:
            user = User(name="Marcelo")
            session.store(user, doc_id)

            ts = session.typed_time_series_for(HeartRateMeasure, doc_id)

            ts.append_single(base_line, HeartRateMeasure(59), "watches/fitbit")
            session.save_changes()

        with self.store.open_session() as session:
            time_series = session.typed_time_series_for(HeartRateMeasure, doc_id).get()

            self.assertEqual(1, len(time_series))
            self.assertEqual(59, time_series[0].value.heart_rate)

            time_series_2 = session.typed_time_series_for(HeartRateMeasure, doc_id).get()

            self.assertEqual(1, len(time_series_2))
            self.assertEqual(59, time_series_2[0].value.heart_rate)

            self.assertEqual(1, session.advanced.number_of_requests)

    def test_include_time_series_and_update_existing_range_in_cache_typed(self):
        base_line = datetime(2023, 8, 20, 21, 30)
        doc_id = "users/0x901507"

        with self.store.open_session() as session:
            user = User(name="Gracjan")
            session.store(user, doc_id)

            session.save_changes()

        with self.store.open_session() as session:
            tsf = session.typed_time_series_for(HeartRateMeasure, doc_id)

            for i in range(360):
                tsf.append_single(base_line + timedelta(seconds=i * 10), HeartRateMeasure(6), "watches/fitbit")

            session.save_changes()

        with self.store.open_session() as session:
            vals = session.typed_time_series_for(HeartRateMeasure, doc_id).get(
                base_line + timedelta(minutes=2), base_line + timedelta(minutes=10)
            )

            self.assertEqual(1, session.advanced.number_of_requests)

            self.assertEqual(49, len(vals))

            self.assertEqual(base_line + timedelta(minutes=2), vals[0].timestamp)
            self.assertEqual(base_line + timedelta(minutes=10), vals[48].timestamp)

            singularity = base_line + timedelta(minutes=3, seconds=3)

            session.typed_time_series_for(HeartRateMeasure, doc_id).append_single(
                singularity, HeartRateMeasure(6), "watches/fitbit"
            )
            session.save_changes()

            self.assertEqual(2, session.advanced.number_of_requests)

            user = session.load(
                doc_id,
                User,
                lambda i: i.include_time_series(
                    "heartRateMeasures", base_line + timedelta(minutes=3), base_line + timedelta(minutes=5)
                ),
            )

            self.assertEqual(3, session.advanced.number_of_requests)

            # should not go to server

            vals = session.typed_time_series_for(HeartRateMeasure, doc_id).get(
                base_line + timedelta(minutes=3), base_line + timedelta(minutes=5)
            )

            self.assertEqual(3, session.advanced.number_of_requests)

            self.assertEqual(14, len(vals))
            self.assertEqual(base_line + timedelta(minutes=3), vals[0].timestamp)
            self.assertEqual(base_line + timedelta(minutes=3, seconds=3), vals[1].timestamp)
            self.assertEqual(base_line + timedelta(minutes=5), vals[13].timestamp)

    def test_can_include_typed_time_series(self):
        base_line = datetime(2023, 8, 20, 21, 30)
        doc_id = "users/0x901507"

        with self.store.open_session() as session:
            user = User(name="Gracjan")
            session.store(user, doc_id)
            ts = session.typed_time_series_for(HeartRateMeasure, doc_id)
            ts.append_single(base_line, HeartRateMeasure(59), "watches/fitbit")

            session.save_changes()

        with self.store.open_session() as session:
            items = list(session.query(object_type=User).include(lambda x: x.include_time_series("HeartRateMeasures")))

            for item in items:
                time_series = session.typed_time_series_for(HeartRateMeasure, item.Id, "HeartRateMeasures").get()
                self.assertEqual(1, len(time_series))
                self.assertEqual(59, time_series[0].value.heart_rate)

            self.assertEqual(1, session.advanced.number_of_requests)

    def test_include_time_series_and_merge_with_existing_ranges_in_cache_typed(self):
        base_line = datetime(2023, 8, 20, 21, 30)
        doc_id = "users/0x901507"

        with self.store.open_session() as session:
            user = User(name="Gracjan")
            session.store(user, doc_id)
            session.save_changes()

        with self.store.open_session() as session:
            tsf = session.typed_time_series_for(HeartRateMeasure, doc_id)

            for i in range(360):
                typed_measure = HeartRateMeasure(6)
                tsf.append_single(base_line + timedelta(seconds=i * 10), typed_measure, "watches/fitbit")

            session.save_changes()

        with self.store.open_session() as session:
            vals = session.typed_time_series_for(HeartRateMeasure, doc_id).get(
                base_line + timedelta(minutes=2), base_line + timedelta(minutes=10)
            )

            self.assertEqual(1, session.advanced.number_of_requests)

            self.assertEqual(49, len(vals))
            self.assertEqual(base_line + timedelta(minutes=2), vals[0].timestamp)
            self.assertEqual(base_line + timedelta(minutes=10), vals[48].timestamp)

            user = session.load(
                doc_id,
                User,
                lambda i: i.include_time_series(
                    "heartRateMeasures", base_line + timedelta(minutes=40), base_line + timedelta(minutes=50)
                ),
            )

            self.assertEqual(2, session.advanced.number_of_requests)

            # should not go to server

            vals = session.typed_time_series_for(HeartRateMeasure, doc_id).get(
                base_line + timedelta(minutes=40), base_line + timedelta(minutes=50)
            )

            self.assertEqual(2, session.advanced.number_of_requests)

            self.assertEqual(61, len(vals))
            self.assertEqual(base_line + timedelta(minutes=40), vals[0].timestamp)
            self.assertEqual(base_line + timedelta(minutes=50), vals[60].timestamp)

            cache = session.time_series_by_doc_id.get(doc_id, None)
            self.assertIsNotNone(cache)

            ranges = cache.get("heartRateMeasures")
            self.assertEqual(2, len(ranges))

            self.assertEqual(base_line + timedelta(minutes=2), ranges[0].from_date)
            self.assertEqual(base_line + timedelta(minutes=10), ranges[0].to_date)
            self.assertEqual(base_line + timedelta(minutes=40), ranges[1].from_date)
            self.assertEqual(base_line + timedelta(minutes=50), ranges[1].to_date)

            # we intentionally evict just the document (without it's TS data),
            # so that Load request will go to server

            session.documents_by_entity.evict(user)
            session.documents_by_id.remove(doc_id)

            # should go to server to get [0, 2] and merge it into existing [2, 10]
            user = session.load(
                doc_id,
                User,
                lambda i: i.include_time_series("heartRateMeasures", base_line, base_line + timedelta(minutes=2)),
            )

            self.assertEqual(3, session.advanced.number_of_requests)

            # should not go to server

            vals = session.typed_time_series_for(HeartRateMeasure, doc_id).get(
                base_line, base_line + timedelta(minutes=2)
            )
            self.assertEqual(3, session.advanced.number_of_requests)

            self.assertEqual(13, len(vals))
            self.assertEqual(base_line, vals[0].timestamp)
            self.assertEqual(base_line + timedelta(minutes=2), vals[12].timestamp)

            self.assertEqual(2, len(ranges))

            self.assertEqual(base_line, ranges[0].from_date)
            self.assertEqual(base_line + timedelta(minutes=10), ranges[0].to_date)
            self.assertEqual(base_line + timedelta(minutes=40), ranges[1].from_date)
            self.assertEqual(base_line + timedelta(minutes=50), ranges[1].to_date)

            # evict just the document

            session.documents_by_entity.evict(user)
            session.documents_by_id.remove(doc_id)

            # should go to server to get [10, 16] and merge it into existing [0, 10]
            user = session.load(
                doc_id,
                User,
                lambda i: i.include_time_series(
                    "heartRateMeasures", base_line + timedelta(minutes=10), base_line + timedelta(minutes=16)
                ),
            )

            self.assertEqual(4, session.advanced.number_of_requests)

            # should not go to server

            vals = session.typed_time_series_for(HeartRateMeasure, doc_id).get(
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
            session.documents_by_id.remove(doc_id)

            # should go to server to get range [17,19]
            # and add it to cache between [10, 16] and [40, 50]

            user = session.load(
                doc_id,
                User,
                lambda i: i.include_time_series(
                    "heartRateMeasures", base_line + timedelta(minutes=17), base_line + timedelta(minutes=19)
                ),
            )

            self.assertEqual(5, session.advanced.number_of_requests)

            # should not go to server

            vals = session.typed_time_series_for(HeartRateMeasure, doc_id).get(
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
            session.documents_by_id.remove(doc_id)

            # should go to server to get range [19, 40]
            # and merge the result with existing ranges [17, 19] and [40, 50]
            # into single range [17, 50]

            user = session.load(
                doc_id,
                User,
                lambda i: i.include_time_series(
                    "heartRateMeasures", base_line + timedelta(minutes=18), base_line + timedelta(minutes=48)
                ),
            )

            self.assertEqual(6, session.advanced.number_of_requests)

            vals = session.typed_time_series_for(HeartRateMeasure, doc_id).get(
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
            session.documents_by_id.remove(doc_id)

            # should go to server to get range [12, 22]
            # and merge the result with existing ranges [10, 16] and [17, 50]
            # into single range [0, 50]

            user = session.load(
                doc_id,
                User,
                lambda i: i.include_time_series(
                    "heartRateMeasures", base_line + timedelta(minutes=12), base_line + timedelta(minutes=22)
                ),
            )

            self.assertEqual(7, session.advanced.number_of_requests)

            # should not go to server

            vals = session.typed_time_series_for(HeartRateMeasure, doc_id).get(
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
            session.documents_by_id.remove(doc_id)

            # should go to server to get range[50, ∞]
            # and merge the result with existing range[0, 50] into single range[0, ∞]
            user = session.load(
                doc_id,
                User,
                lambda i: i.include_time_series_by_range_type_and_time(
                    "heartRateMeasures", TimeSeriesRangeType.LAST, TimeValue.of_minutes(10)
                ),
            )

            self.assertEqual(8, session.advanced.number_of_requests)

            # should not go to server

            vals = session.typed_time_series_for(HeartRateMeasure, doc_id).get(base_line + timedelta(minutes=50), None)

            self.assertEqual(8, session.advanced.number_of_requests)

            self.assertEqual(60, len(vals))

            self.assertEqual(base_line + timedelta(minutes=50), vals[0].timestamp)
            self.assertEqual(base_line + timedelta(minutes=59, seconds=50), vals[59].timestamp)

            self.assertEqual(1, len(ranges))

            self.assertEqual(base_line, ranges[0].from_date)
            self.assertIsNone(ranges[0].to_date)

    @unittest.skip("Insufficient license permissions. Skipping on CI/CD.")
    def test_can_serve_time_series_from_cache_rollup(self):
        raw = RawTimeSeriesPolicy(TimeValue.of_hours(24))

        p1 = TimeSeriesPolicy("By6Hours", TimeValue.of_hours(6), TimeValue.of_days(4))
        p2 = TimeSeriesPolicy("By1Day", TimeValue.of_days(1), TimeValue.of_days(5))
        p3 = TimeSeriesPolicy("By30Minutes", TimeValue.of_minutes(30), TimeValue.of_days(2))
        p4 = TimeSeriesPolicy("By1Hour", TimeValue.of_hours(1), TimeValue.of_days(3))

        time_series_collection_configuration = TimeSeriesCollectionConfiguration()
        time_series_collection_configuration.raw_policy = raw
        time_series_collection_configuration.policies = [p1, p2, p3, p4]

        config = TimeSeriesConfiguration()
        config.collections = {"users": time_series_collection_configuration}
        config.policy_check_frequency = timedelta(seconds=1)

        self.store.maintenance.send(ConfigureTimeSeriesOperation(config))
        self.store.time_series.register_type(User, StockPrice)

        total = TimeValue.of_days(12).value
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

        time.sleep(1.2)

        with self.store.open_session() as session:
            ts = session.time_series_rollup_for(StockPrice, "users/karmel", p1.name)
            res = ts.get()

            self.assertEqual(16, len(res))

            # should not go to server
            res = ts.get(base_line, base_line + timedelta(days=365))
            self.assertEqual(16, len(res))
            self.assertEqual(1, session.advanced.number_of_requests)

    @unittest.skip("Insufficient license permissions. Skipping on CI/CD.")
    def test_can_include_typed_time_series_rollup(self):
        raw = RawTimeSeriesPolicy(TimeValue.of_hours(24))

        p1 = TimeSeriesPolicy("By6Hours", TimeValue.of_hours(6), TimeValue.of_days(4))
        p2 = TimeSeriesPolicy("By1Day", TimeValue.of_days(1), TimeValue.of_days(5))
        p3 = TimeSeriesPolicy("By30Minutes", TimeValue.of_minutes(30), TimeValue.of_days(2))
        p4 = TimeSeriesPolicy("By1Hour", TimeValue.of_hours(1), TimeValue.of_days(3))

        time_series_collection_configuration = TimeSeriesCollectionConfiguration()
        time_series_collection_configuration.raw_policy = raw
        time_series_collection_configuration.policies = [p1, p2, p3, p4]

        config = TimeSeriesConfiguration()
        config.collections = {"users": time_series_collection_configuration}
        config.policy_check_frequency = timedelta(seconds=1)

        self.store.maintenance.send(ConfigureTimeSeriesOperation(config))
        self.store.time_series.register_type(User, StockPrice)

        total = TimeValue.of_days(12).value
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

        time.sleep(1.2)

        with self.store.open_session() as session:
            user = (
                session.query(object_type=User)
                .include(lambda i: i.include_time_series(f"stockPrices@{p1.name}"))
                .first()
            )

            # should not go to server
            res = session.time_series_rollup_for(StockPrice, user.Id, p1.name).get()
            self.assertEqual(16, len(res))
            self.assertEqual(1, session.advanced.number_of_requests)

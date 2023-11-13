from __future__ import annotations
from datetime import datetime, timedelta
from typing import Dict, Tuple, Optional

from ravendb.documents.session.time_series import ITimeSeriesValuesBindable
from ravendb.tests.test_base import TestBase, User


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

            ts.append(base_line, HeartRateMeasure(59), "watches/fitbit")
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
            user = User("Gracjan")
            session.store(user, doc_id)

            session.save_changes()

        with self.store.open_session() as session:
            tsf = session.typed_time_series_for(HeartRateMeasure, doc_id)

            for i in range(360):
                tsf.append(base_line + timedelta(seconds=i * 10), HeartRateMeasure(6), "watches/fitbit")

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

            session.typed_time_series_for(HeartRateMeasure, doc_id).append(
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

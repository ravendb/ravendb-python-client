from __future__ import annotations
from datetime import datetime
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

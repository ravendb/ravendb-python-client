from datetime import timedelta

from ravendb.infrastructure.entities import User
from ravendb.tests.jvm_migrated_tests.client_tests.time_series_tests.test_time_series_typed_session import (
    HeartRateMeasure,
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

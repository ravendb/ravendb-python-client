from datetime import timedelta

from ravendb.infrastructure.entities import User
from ravendb.tests.test_base import TestBase
from ravendb.tools.raven_test_helper import RavenTestHelper


class TestTimeSeriesBulkInsert(TestBase):
    def setUp(self):
        super().setUp()

    def test_should_delete_time_series_upon_document_deletion(self):
        base_line = RavenTestHelper.utc_this_month()
        document_id = "users/ayende"

        with self.store.bulk_insert() as bulk_insert:
            user = User()
            user.name = "Oren"
            bulk_insert.store_as(user, document_id)

            with bulk_insert.time_series_for(document_id, "Heartrate") as time_series_bulk_insert:
                time_series_bulk_insert.append_single(base_line + timedelta(minutes=1), 59, "watches/fitbit")
                time_series_bulk_insert.append_single(base_line + timedelta(minutes=2), 59, "watches/fitbit")

            with bulk_insert.time_series_for(document_id, "Heartrate2") as time_series_bulk_insert_2:
                time_series_bulk_insert_2.append_single(base_line + timedelta(minutes=1), 59, "watches/apple")

        with self.store.open_session() as session:
            session.delete(document_id)
            session.save_changes()

        with self.store.open_session() as session:
            vals = session.time_series_for(document_id, "Heartrate").get(None, None)
            self.assertIsNone(vals)

            vals = session.time_series_for(document_id, "Heartrate2").get(None, None)
            self.assertIsNone(vals)

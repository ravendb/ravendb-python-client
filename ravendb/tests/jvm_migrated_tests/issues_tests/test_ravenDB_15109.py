from datetime import datetime

from ravendb.infrastructure.entities import User
from ravendb.tests.test_base import TestBase


class TestRavenDB15109(TestBase):
    def setUp(self):
        super().setUp()

    def test_bulk_increment_new_time_series_should_add_time_series_name_to_metadata(self):
        with self.store.bulk_insert() as bulk_insert:
            user = User(name="Aviv1")
            bulk_insert.store(user)

            id_ = user.Id

            for i in range(1, 11):
                with bulk_insert.time_series_for(id_, str(i)) as time_series:
                    time_series.append_single(datetime.utcnow(), i)

        with self.store.open_session() as session:
            for i in range(1, 11):
                all_ = session.time_series_for(id_, str(i)).get()
                self.assertEqual(1, len(all_))

        with self.store.open_session() as session:
            u = session.load(id_, User)
            time_series = session.advanced.get_time_series_for(u)
            self.assertIsNotNone(time_series)
            self.assertEqual(10, len(time_series))

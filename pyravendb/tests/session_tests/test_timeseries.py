from pyravendb.tests.test_base import TestBase
from datetime import datetime, timedelta
import unittest


class User:
    def __init__(self, doc_id, name):
        self.Id = doc_id
        self.name = name


class TestTimeSeries(TestBase):
    def setUp(self):
        super().setUp()

        with self.store.open_session() as session:
            session.store(User("users/1-A", "Idan"))
            session.save_changes()

    def tearDown(self):
        super().tearDown()
        self.delete_all_topology_files()

    def test_time_series_append(self):
        now = datetime.now()
        with self.store.open_session() as session:
            tsf = session.time_series_for("users/1-A", "HeartRate")
            tsf.append(now, 70, tag="foo/bar")
            tsf.append(now - timedelta(hours=3), 86, tag="foo/bazz")
            session.save_changes()

            time_series = tsf.get()
            self.assertEqual(len(time_series), 2)
            self.assertTrue(time_series[0].value == 86 and time_series[1].value == 70)

    def test_time_series_removal(self):
        now = datetime.now()
        with self.store.open_session() as session:
            tsf = session.time_series_for("users/1-A", "HeartRate")
            tsf.append(now, 70, tag="foo/bar")
            session.save_changes()
            time_series = tsf.get()
            self.assertEqual(len(time_series), 1)

        with self.store.open_session() as session:
            tsf = session.time_series_for("users/1-A", "HeartRate")
            tsf.remove(now, datetime.max)
            session.save_changes()

            time_series = tsf.get()
            self.assertTrue(not time_series)

    def test_time_series_cache(self):
        now = datetime.now()
        with self.store.open_session() as session:
            tsf = session.time_series_for("users/1-A", "HeartRate")
            tsf.append(now + timedelta(minutes=5), 70, tag="foo/bar")
            tsf.append(now + timedelta(hours=3), 71, tag="foo/barz")
            tsf.append(now + timedelta(days=5), 72, tag="foo/barzz")

            session.save_changes()

        with self.store.open_session() as session:
            tsf = session.time_series_for("users/1-A", "HeartRate")
            tsf.get(from_date=now, to_date=now + timedelta(hours=1))
            tsf.get(now + timedelta(hours=2), now + timedelta(days=3))
            tsf.get(now + timedelta(days=4), now + timedelta(days=6))
            tsf.get(now + timedelta(days=3), now + timedelta(days=5))

            self.assertEqual(session.advanced.number_of_requests_in_session(), 4)

            tsf.get(now + timedelta(days=2), now + timedelta(days=6))
            self.assertEqual(session.advanced.number_of_requests_in_session(), 4)


if __name__ == '__main__':
    unittest.main()

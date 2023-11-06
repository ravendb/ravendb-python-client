from ravendb.tests.test_base import TestBase, User
from datetime import datetime, timedelta


class TestTimeSeries(TestBase):
    def setUp(self):
        super().setUp()

        with self.store.open_session() as session:
            session.store(User("users/1-A", "Idan"))
            session.save_changes()

    def tearDown(self):
        super().tearDown()

    def test_time_series_append(self):
        base = datetime.now()
        with self.store.open_session() as session:
            tsf = session.time_series_for("users/1-A", "HeartRate")
            tsf.append_single(base, 70, "foo/bar")
            tsf.append_single(base - timedelta(hours=3), 86, "foo/bazz")
            session.save_changes()

            time_series = tsf.get()
            self.assertEqual(2, len(time_series))
            self.assertEqual(86, time_series[0].value)
            self.assertEqual(70, time_series[1].value)

    def test_time_series_removal(self):
        base = datetime.now()
        with self.store.open_session() as session:
            tsf = session.time_series_for("users/1-A", "HeartRate")
            tsf.append_single(base, 70, "foo/bar")
            session.save_changes()

            time_series = tsf.get()
            self.assertEqual(1, len(time_series))

        with self.store.open_session() as session:
            tsf = session.time_series_for("users/1-A", "HeartRate")
            tsf.delete(base, datetime.max)
            session.save_changes()

            time_series = tsf.get()
            self.assertIsNone(time_series)

    def test_time_series_cache(self):
        base = datetime.now()
        with self.store.open_session() as session:
            tsf = session.time_series_for("users/1-A", "HeartRate")
            tsf.append_single(base + timedelta(minutes=5), 70, "foo/bar")
            tsf.append_single(base + timedelta(hours=3), 71, "foo/barz")
            tsf.append_single(base + timedelta(days=5), 72, "foo/barzz")

            session.save_changes()

        with self.store.open_session() as session:
            tsf = session.time_series_for("users/1-A", "HeartRate")
            tsf.get(base, base + timedelta(hours=1))
            tsf.get(base + timedelta(hours=2), base + timedelta(days=3))
            tsf.get(base + timedelta(days=4), base + timedelta(days=6))
            tsf.get(base + timedelta(days=3), base + timedelta(days=5))

            self.assertEqual(session.advanced.number_of_requests, 4)

            tsf.get(base + timedelta(days=2), base + timedelta(days=6))
            self.assertEqual(session.advanced.number_of_requests, 4)

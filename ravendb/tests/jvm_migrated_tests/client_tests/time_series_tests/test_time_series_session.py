from datetime import datetime, timedelta

from ravendb.infrastructure.entities import User
from ravendb.tests.test_base import TestBase


class TestTimeSeriesSession(TestBase):
    def setUp(self):
        super(TestTimeSeriesSession, self).setUp()

    def test_can_create_simple_time_series(self):
        today = datetime.today()
        base_line = datetime(today.year, today.month, today.day)

        with self.store.open_session() as session:
            user = User(name="Oren")
            session.store(user, "users/ayende")
            timestamp = base_line + timedelta(minutes=1)
            session.time_series_for("users/ayende", "Heartrate").append(timestamp, 59, "watches/fitbit")
            session.save_changes()

        with self.store.open_session() as session:
            val = session.time_series_for("users/ayende", "Heartrate").get()[0]
            self.assertEqual([59], val.values)
            self.assertEqual("watches/fitbit", val.tag)
            self.assertEqual(timestamp, val.timestamp)

    def test_can_crate_simple_time_series_2(self):
        today = datetime.today()
        base_line = datetime(today.year, today.month, today.day)

        with self.store.open_session() as session:
            user = User(name="Oren")
            session.store(user, "users/ayende")

            tsf = session.time_series_for("users/ayende", "Heartrate")
            tsf.append(base_line + timedelta(1), 59, "watches/fitbit")
            tsf.append(base_line + timedelta(2), 60, "watches/fitbit")
            tsf.append(base_line + timedelta(3), 61, "watches/fitbit")

            session.save_changes()

        with self.store.open_session() as session:
            val = session.time_series_for("users/ayende", "Heartrate").get()
            self.assertEqual(3, len(val))

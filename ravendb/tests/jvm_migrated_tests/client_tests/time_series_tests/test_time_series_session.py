import datetime

from ravendb.infrastructure.entities import User
from ravendb.tests.test_base import TestBase


class TestTimeSeriesSession(TestBase):
    def setUp(self):
        super(TestTimeSeriesSession, self).setUp()

    def test_can_create_simple_time_series(self):
        today = datetime.datetime.today()
        base_line = datetime.datetime(today.year, today.month, today.day)

        with self.store.open_session() as session:
            user = User(name="Oren")
            session.store(user, "users/ayende")
            timestamp = base_line + datetime.timedelta(minutes=1)
            session.time_series_for("users/ayende", "Heartrate").append(timestamp, 59, "watches/fitbit")
            session.save_changes()

        with self.store.open_session() as session:
            val = session.time_series_for("users/ayende", "Heartrate").get()[0]
            self.assertEqual([59], val.values)
            self.assertEqual("watches/fitbit", val.tag)
            self.assertEqual(timestamp, val.timestamp)

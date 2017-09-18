from pyravendb.tests.test_base import TestBase
from datetime import datetime, timedelta
from pyravendb.tools.utils import Utils
import unittest


class Time(object):
    def __init__(self, td, dt):
        self.td = td
        self.dt = dt


class TestConversion(TestBase):
    def setUp(self):
        super(TestConversion, self).setUp()

        with self.store.open_session() as session:
            session.store(Time(Utils.timedelta_to_str(timedelta(days=20, minutes=23, seconds=59, milliseconds=254)),
                               Utils.datetime_to_string(datetime.now())), "times/3")
            session.store(Time(Utils.timedelta_to_str(timedelta(minutes=23, seconds=59, milliseconds=254)),
                               Utils.datetime_to_string(datetime.now())), "times/4")
            session.save_changes()

    def tearDown(self):
        super(TestConversion, self).tearDown()
        self.delete_all_topology_files()

    def test_load_timedelta_and_datetime(self):
        with self.store.open_session() as session:
            times = session.load("times/3", object_type=Time, nested_object_types={"td": timedelta, "dt": datetime})
            self.assertTrue(isinstance(times.td, timedelta) and isinstance(times.dt, datetime))

    def test_store_conversion(self):
        with self.store.open_session() as session:
            times = Time(timedelta(days=1, hours=1), datetime.now())
            session.store(times)
            key = session.advanced.get_document_id(times)
            session.save_changes()

        with self.store.open_session() as session:
            times = session.load(key, object_type=Time, nested_object_types={"td": timedelta, "dt": datetime})
            self.assertTrue(isinstance(times.td, timedelta) and isinstance(times.dt, datetime))

    def test_query_conversion(self):
        with self.store.open_session() as session:
            query = list(session.query(object_type=Time, nested_object_types={"td": timedelta,
                                                                              "dt": datetime}).
                         where_greater_than_or_equal("td", timedelta(days=9)))

            not_working = False
            if len(query) < 1:
                not_working = True
            for item in query:
                if not isinstance(item.td, timedelta):
                    not_working = True

            self.assertFalse(not_working)
            self.assertEqual(len(query), 1)


if __name__ == "__main__":
    unittest.main()

from pyravendb.tests.test_base import TestBase
from pyravendb.tools.utils import Utils
from datetime import timedelta


class TestJsonExtensions(TestBase):
    def setUp(self):
        super(TestJsonExtensions, self).setUp()

    def test_can_serialize_duration(self):
        self.assertEqual("00:00:00", Utils.timedelta_to_str(timedelta()))
        self.assertEqual("00:00:02", Utils.timedelta_to_str(timedelta(seconds=2)))
        self.assertEqual("00:03:00", Utils.timedelta_to_str(timedelta(minutes=3)))
        self.assertEqual("04:00:00", Utils.timedelta_to_str(timedelta(hours=4)))
        self.assertEqual("1.00:00:00", Utils.timedelta_to_str(timedelta(days=1)))
        self.assertEqual("2.05:03:07", Utils.timedelta_to_str(timedelta(days=2, hours=5, minutes=3, seconds=7)))
        self.assertEqual("00:00:00.002000", Utils.timedelta_to_str(timedelta(milliseconds=2)))

    def test_can_deserialize_duration(self):
        self.assertEqual("0:00:01", str(Utils.string_to_timedelta("00:00:01")))
        self.assertEqual("0:00:00", str(Utils.string_to_timedelta("00:00:00")))
        self.assertEqual("2 days, 0:00:01", str(Utils.string_to_timedelta("2.00:00:01")))
        self.assertEqual("0:00:00.001234", str(Utils.string_to_timedelta("00:00:00.1234")))

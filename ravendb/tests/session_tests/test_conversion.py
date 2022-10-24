from ravendb.documents.session.event_args import BeforeStoreEventArgs
from ravendb.tests.test_base import TestBase
from datetime import datetime, timedelta
from ravendb.tools.utils import Utils


class Time(object):
    def __init__(self, td, dt):
        self.td = td
        self.dt = dt


class Item(object):
    def __init__(self, val):
        self.val = val


class TestConversion(TestBase):
    def setUp(self):
        super(TestConversion, self).setUp()

        with self.store.open_session() as session:
            session.store(
                Time(
                    Utils.timedelta_to_str(timedelta(days=20, minutes=23, seconds=59, milliseconds=254)),
                    Utils.datetime_to_string(datetime.now()),
                ),
                "times/3",
            )
            session.store(
                Time(
                    Utils.timedelta_to_str(timedelta(minutes=23, seconds=59, milliseconds=254)),
                    Utils.datetime_to_string(datetime.now()),
                ),
                "times/4",
            )
            session.save_changes()

    def tearDown(self):
        super(TestConversion, self).tearDown()
        self.delete_all_topology_files()

    def test_before_store(self):
        def update_item(args: BeforeStoreEventArgs):
            args.entity.val = 2

        with self.store.open_session() as session:
            session.add_before_store(update_item)
            session.store(Item(1), "item/1")
            session.save_changes()

        with self.store.open_session() as session:
            time = session.load("item/1", object_type=Item)
            self.assertEqual(2, time.val)

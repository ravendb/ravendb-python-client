from typing import List

from ravendb.documents.session.event_args import BeforeQueryEventArgs

from ravendb.tests.test_base import TestBase


class AA:
    def __init__(self, id: str, bs: List[str]):
        self.id = id
        self.bs = bs


class B:
    def __init__(self, id: str):
        self.id = id


class TestQuery(TestBase):
    def setUp(self):
        super().setUp()

    def create_data(self):
        with self.store.open_session() as session:
            a = AA("a/1", ["b/1"])
            b = B("b/1")
            session.store(a)
            session.store(b)
            session.save_changes()

    def test_can_load_entities_with_no_tracking(self):
        self.create_data()

        def on_before_query(event_args: BeforeQueryEventArgs):
            query_to_be_executed = event_args.query_customization.query
            query_to_be_executed.no_tracking()

        with self.store.open_session() as session:
            session.add_before_query(on_before_query)

            result: List[AA] = list(session.query(object_type=AA).include("bs"))

            self.assertEqual(1, len(result))

            result[0].bs.clear()

            self.assertFalse(session.advanced.has_changed(result[0]))

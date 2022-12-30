import time

from ravendb.tests.test_base import TestBase


class FooBar:
    def __init__(self, name: str = None):
        self.name = name


class TestBulkInserts(TestBase):
    def setUp(self):
        super(TestBulkInserts, self).setUp()

    def test_simple_bulk_insert_should_work(self):
        foo_bar1 = FooBar("John Doe")
        foo_bar2 = FooBar("Jane Doe")
        foo_bar3 = FooBar("Mega John")
        foo_bar4 = FooBar("Mega Jane")

        with self.store.bulk_insert() as bulk_insert:
            bulk_insert.store_by_entity(foo_bar1)
            bulk_insert.store_by_entity(foo_bar2)
            bulk_insert.store_by_entity(foo_bar3)
            bulk_insert.store_by_entity(foo_bar4)

        time.sleep(5)

        with self.store.open_session() as session:
            doc1 = session.load("FooBars/1-A", FooBar)
            doc2 = session.load("FooBars/2-A", FooBar)
            doc3 = session.load("FooBars/3-A", FooBar)
            doc4 = session.load("FooBars/4-A", FooBar)

            self.assertIsNotNone(doc1)
            self.assertIsNotNone(doc2)
            self.assertIsNotNone(doc3)
            self.assertIsNotNone(doc4)

            self.assertEqual("John Doe", doc1.name)
            self.assertEqual("Jane Doe", doc2.name)
            self.assertEqual("Mega John", doc3.name)
            self.assertEqual("Mega Jane", doc4.name)

    def test_should_not_acept_ids_ending_with_pipe_line(self):
        with self.store.bulk_insert() as bulk_insert:
            self.assertRaisesWithMessage(
                bulk_insert.store,
                RuntimeError,
                "Document ids cannot end with '|', but was called with foobars|",
                FooBar(),
                "foobars|",
            )

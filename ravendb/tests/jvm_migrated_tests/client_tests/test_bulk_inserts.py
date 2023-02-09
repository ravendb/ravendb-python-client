import datetime
import time

from ravendb import MetadataAsDictionary, constants
from ravendb.exceptions.documents.bulkinsert import BulkInsertAbortedException
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

    def test_should_not_accept_ids_ending_with_pipe_line(self):
        with self.store.bulk_insert() as bulk_insert:
            self.assertRaisesWithMessage(
                bulk_insert.store,
                RuntimeError,
                "Document ids cannot end with '|', but was called with foobars|",
                FooBar(),
                "foobars|",
            )

    def test_can_modify_metadata_with_bulk_insert(self):
        expiration_date = (datetime.datetime.utcnow() + datetime.timedelta(days=365)).isoformat() + "0Z"  # add one year

        with self.store.bulk_insert() as bulk_insert:
            foobar = FooBar("Jon Snow")
            metadata = MetadataAsDictionary()
            metadata[constants.Documents.Metadata.EXPIRES] = expiration_date
            bulk_insert.store_by_entity(foobar, metadata)

        with self.store.open_session() as session:
            entity = session.load("FooBars/1-A", FooBar)
            metadata_expiration_date = session.advanced.get_metadata_for(entity)[constants.Documents.Metadata.EXPIRES]
            self.assertEqual(expiration_date, metadata_expiration_date)

    def test_killed_to_early(self):
        with self.assertRaises(BulkInsertAbortedException):
            with self.store.bulk_insert() as bulk_insert:
                bulk_insert.store_by_entity(FooBar())
                time.sleep(0.1)  # todo: wait for operation to be created first, then try to kill it
                bulk_insert.abort()
                bulk_insert.store_by_entity(FooBar())

from ravendb import DocumentStore
from ravendb.tests.test_base import TestBase


class TestRavenDB15706(TestBase):
    def setUp(self):
        super(TestRavenDB15706, self).setUp()

    def test_bulk_insert_without_db(self):
        with DocumentStore(self.store.urls) as store:
            store.initialize()
            with self.assertRaises(ValueError):
                store.bulk_insert()

from pyravendb.tests.test_base import TestBase
from pyravendb.store.document_store import DocumentStore
from pyravendb.custom_exceptions import exceptions
import unittest


class Foo(object):
    def __init__(self, name, key):
        self.name = name
        self.key = key
        # self.Id = None


class TestSessionStore(TestBase):
    @classmethod
    def setUpClass(cls):
        super(TestSessionStore, cls).setUpClass()
        cls.document_store = DocumentStore(cls.default_url, cls.default_database)
        cls.document_store.initialize()

    def test_store_without_key(self):
        foo = Foo("test", 10)
        with self.document_store.open_session() as session:
            session.store(foo)
            session.save_changes()

        with self.document_store.open_session() as session:
            self.assertIsNotNone(session.load(foo.Id))

    def test_store_with_key(self):
        foo = Foo("test", 20)
        with self.document_store.open_session() as session:
            session.store(foo, "testingStore/1")
            session.save_changes()

        with self.document_store.open_session() as session:
            self.assertEqual(session.load("testingStore/1").key, 20)

    def test_store_after_delete_fail(self):
        foo = Foo("test", 20)
        with self.document_store.open_session() as session:
            session.store(foo, "testingStore")
            session.save_changes()

        with self.document_store.open_session() as session:
            session.delete("testingStore")
            with self.assertRaises(exceptions.InvalidOperationException):
                session.store(foo)


if __name__ == "__main__":
    unittest.main()

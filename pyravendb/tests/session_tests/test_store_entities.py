from pyravendb.tests.test_base import TestBase
from pyravendb.store.document_store import DocumentStore
from pyravendb.custom_exceptions import exceptions
import unittest


class Foo(object):
    def __init__(self, name, key):
        self.name = name
        self.key = key


class TestSessionStore(TestBase):
    def setUp(self):
        super(TestSessionStore, self).setUp()

    def tearDown(self):
        super(TestSessionStore, self).tearDown()
        self.delete_all_topology_files()

    def test_store_without_key(self):
        foo = Foo("test", 10)
        with self.store.open_session() as session:
            session.store(foo)
            session.save_changes()

        with self.store.open_session() as session:
            self.assertIsNotNone(session.load("foos/1-A"))

    def test_store_with_key(self):
        foo = Foo("test", 20)
        with self.store.open_session() as session:
            session.store(foo, "testingStore/1-A")
            session.save_changes()

        with self.store.open_session() as session:
            self.assertEqual(session.load("testingStore/1-A").key, 20)

    def test_store_after_delete_fail(self):
        foo = Foo("test", 20)
        with self.store.open_session() as session:
            session.store(foo, "testingStore")
            session.save_changes()

        with self.store.open_session() as session:
            session.delete("testingStore")
            with self.assertRaises(exceptions.InvalidOperationException):
                session.store(foo, "testingStore")

    def _save_documents(self, documents):
        with self.store.open_session() as session:
            for document in documents:
                session.store(document)
            session.save_changes()

    def test_store_the_same_documents_should_work(self):
        documents = []
        for i in range(0, 40):
            documents.append(Foo("IdanHaim", i))

        self._save_documents(documents)
        self._save_documents(documents)
        self._save_documents(documents)
        self._save_documents(documents)

        with self.store.open_session() as session:
            results = list(session.query().raw_query("From Foos"))
            self.assertEqual(len(results), 40 * 4)


if __name__ == "__main__":
    unittest.main()

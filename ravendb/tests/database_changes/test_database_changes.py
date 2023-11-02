from ravendb.documents.indexes.definitions import IndexDefinition
from ravendb.documents.operations.indexes import PutIndexesOperation
from ravendb.tests.test_base import TestBase
from ravendb.changes.observers import ActionObserver
from datetime import datetime, timedelta
import unittest
from threading import Event
from time import sleep


class User:
    def __init__(self, name):
        self.name = name


class Address:
    def __init__(self, country):
        self.country = country


class TestDatabaseChanges(TestBase):
    def setUp(self):
        super(TestDatabaseChanges, self).setUp()

    def tearDown(self):
        super(TestDatabaseChanges, self).tearDown()

    def test_for_all_documents(self):
        event = Event()
        all_documents_changes = []

        def on_next(value):
            all_documents_changes.append(value)
            if len(all_documents_changes) == 9:
                event.set()

        all_observer = self.store.changes().for_all_documents()
        close_method = all_observer.subscribe_with_observer(ActionObserver(on_next=on_next))
        all_observer.ensure_subscribe_now()
        with self.store.open_session() as session:
            session.store(User("Idan"))
            session.store(User("Shalom"))
            session.store(User("Ilay"))
            session.store(User("Agam"))
            session.store(User("Lee"))
            session.store(User("Oren"))
            session.store(User("Ayende"))
            session.store(User("Raven"))
            session.save_changes()
        event.wait(10)
        self.assertEqual(9, len(all_documents_changes))
        close_method()

    def test_for_document(self):
        documents = []
        observer = self.store.changes().for_document("users/1")
        close_method = observer.subscribe(documents.append)
        observer.ensure_subscribe_now()

        with self.store.open_session() as session:
            session.store(User("Idan"), key="users/1")
            session.store(User("Shalom"), key="users/2")
            session.save_changes()

        sleep(1)
        self.assertTrue(len(documents) == 1)
        self.assertEqual("users/1", documents[0].key)
        self.assertEqual("Put", str(documents[0].type_of_change))
        self.assertIsNotNone(documents[0].change_vector)
        close_method()

    def test_document_change(self):
        documents = []
        observer = self.store.changes().for_document("users/1")
        close_method = observer.subscribe(documents.append)
        observer.ensure_subscribe_now()

        with self.store.open_session() as session:
            session.store(User("Idan"), key="users/1")
            session.save_changes()

        with self.store.open_session() as session:
            user = session.load("users/1")
            user.name = "Ilay"
            session.save_changes()

        sleep(1)
        self.assertTrue(len(documents) == 2)
        self.assertEqual(documents[1].key, documents[0].key)
        self.assertNotEqual(documents[1].change_vector, documents[0].change_vector)
        close_method()

    def test_for_all_indexes(self):
        indexes_change = []
        observer = self.store.changes().for_all_indexes()
        close_method = observer.subscribe(indexes_change.append)
        observer.ensure_subscribe_now()

        index_definition_dogs = IndexDefinition()
        index_definition_dogs.name = "Dogs"
        index_definition_dogs.maps = ["from doc in docs.Dogs select new {doc.brand}"]

        self.store.maintenance.send(PutIndexesOperation(index_definition_dogs))

        sleep(1)
        self.assertTrue(len(indexes_change) >= 1)
        self.assertEqual(indexes_change[0].name, "Dogs")
        self.assertEqual(str(indexes_change[0].type_of_change), "IndexAdded")
        close_method()

    def test_for_index(self):
        index_change = []
        observer = self.store.changes().for_index("Dogs")
        close_method = observer.subscribe(index_change.append)
        observer.ensure_subscribe_now()

        index_definition_users = IndexDefinition()
        index_definition_users.name = "Users"
        index_definition_users.maps = ["from doc in docs.Users select new {doc.Name}"]

        index_definition_dogs = IndexDefinition()
        index_definition_dogs.name = "Dogs"
        index_definition_dogs.maps = ["from doc in docs.Dogs select new {doc.brand}"]

        self.store.maintenance.send(PutIndexesOperation(index_definition_dogs, index_definition_users))

        sleep(1)
        for index in index_change:
            self.assertFalse(index.name != "Dogs")

        close_method()

    def test_subscribe_to_index_and_document(self):
        documents = []
        indexes = []

        observer = self.store.changes().for_document("users/1")
        close_method1 = observer.subscribe(documents.append)
        observer.ensure_subscribe_now()

        observer = self.store.changes().for_index("Dogs")
        close_method2 = observer.subscribe(indexes.append)
        observer.ensure_subscribe_now()

        index_definition_users = IndexDefinition()
        index_definition_users.name = "Users"
        index_definition_users.maps = ["from doc in docs.Users select new {doc.Name}"]

        index_definition_dogs = IndexDefinition()
        index_definition_dogs.name = "Dogs"
        index_definition_dogs.maps = ["from doc in docs.Dogs select new {doc.brand}"]

        self.store.maintenance.send(PutIndexesOperation(index_definition_dogs, index_definition_users))

        with self.store.open_session() as session:
            session.store(User("Idan"), key="users/1")
            session.save_changes()

        sleep(1)
        self.assertTrue(len(documents) >= 1)
        self.assertTrue(len(indexes) >= 1)
        close_method1()
        close_method2()

    def test_for_documents_start_with(self):
        documents = []
        observer = self.store.changes().for_documents_start_with("users")
        close_method = observer.subscribe(documents.append)
        observer.ensure_subscribe_now()

        with self.store.open_session() as session:
            session.store(User("Idan"))
            session.store(User("Shalom"))
            session.store(User("Ayende"))
            session.store(Address("Israel"))
            session.save_changes()

        sleep(1)
        observer.ensure_subscribe_now()
        self.assertEqual(3, len(documents))
        for document in documents:
            self.assertTrue(document.key.lower().startswith("users"))

        close_method()


if __name__ == "__main__":
    unittest.main()

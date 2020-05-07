from pyravendb.tests.test_base import TestBase
from pyravendb.data.indexes import IndexDefinition
from pyravendb.raven_operations.maintenance_operations import PutIndexesOperation
from pyravendb.changes.observers import ActionObserver
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
        self.delete_all_topology_files()

    def test_for_all_documents(self):
        event = Event()
        all_documents_changes = []

        def on_next(value):
            all_documents_changes.append(value)
            if len(all_documents_changes) == 9:
                event.set()

        all_observer = self.store.changes().for_all_documents()
        all_observer.subscribe(ActionObserver(on_next=on_next))
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

    def test_for_document(self):
        documents = []
        observer = self.store.changes().for_document("users/1")
        observer.subscribe(documents.append)
        observer.ensure_subscribe_now()

        with self.store.open_session() as session:
            session.store(User("Idan"), key="users/1")
            session.store(User("Shalom"), key="users/2")
            session.save_changes()

        sleep(1)
        self.assertTrue(len(documents) == 1)
        self.assertEqual("users/1", documents[0]["Id"])
        self.assertEqual('Put', documents[0]["Type"])
        self.assertIsNotNone(documents[0]["ChangeVector"])

    def test_document_change(self):
        documents = []
        observer = self.store.changes().for_document("users/1")
        observer.subscribe(documents.append)
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
        self.assertEqual(documents[1]["Id"], documents[0]["Id"])
        self.assertNotEqual(documents[1]["ChangeVector"], documents[0]["ChangeVector"])

    def test_for_all_indexes(self):
        indexes_change = []
        observer = self.store.changes().for_all_indexes()
        observer.subscribe(indexes_change.append)
        observer.ensure_subscribe_now()

        index_definition_dogs = IndexDefinition("Dogs", "from doc in docs.Dogs select new {doc.brand}")

        self.store.maintenance.send(PutIndexesOperation(index_definition_dogs))

        sleep(1)
        self.assertTrue(len(indexes_change) >= 1)
        self.assertEqual(indexes_change[0]['Name'], 'Dogs')
        self.assertEqual(indexes_change[0]['Type'], 'IndexAdded')

    def test_for_index(self):
        index_change = []
        observer = self.store.changes().for_index('Dogs')
        observer.subscribe(index_change.append)
        observer.ensure_subscribe_now()

        index_definition_users = IndexDefinition("Users", "from doc in docs.Users select new {doc.Name}")
        index_definition_dogs = IndexDefinition("Dogs", "from doc in docs.Dogs select new {doc.brand}")

        self.store.maintenance.send(PutIndexesOperation(index_definition_dogs, index_definition_users))

        sleep(1)
        for index in index_change:
            self.assertFalse(index['Name'] != 'Dogs')

    def test_subscribe_to_index_and_document(self):
        documents = []
        indexes = []

        observer = self.store.changes().for_document("users/1")
        observer.subscribe(documents.append)
        observer.ensure_subscribe_now()

        observer = self.store.changes().for_index('Dogs')
        observer.subscribe(indexes.append)
        observer.ensure_subscribe_now()

        index_definition_users = IndexDefinition("Users", "from doc in docs.Users select new {doc.Name}")
        index_definition_dogs = IndexDefinition("Dogs", "from doc in docs.Dogs select new {doc.brand}")

        self.store.maintenance.send(PutIndexesOperation(index_definition_dogs, index_definition_users))

        with self.store.open_session() as session:
            session.store(User("Idan"), key="users/1")
            session.save_changes()

        sleep(1)
        self.assertTrue(len(documents) >= 1)
        self.assertTrue(len(indexes) >= 1)

    def test_for_documents_start_with(self):
        documents = []
        observer = self.store.changes().for_documents_start_with("users")
        observer.subscribe(documents.append)
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
            self.assertTrue(document['Id'].startswith('users'))

    def test_for_all_time_series(self):
        event = Event()
        changes = []

        def on_next(value):
            changes.append(value)
            if len(changes) == 2:
                event.set()

        with self.store.open_session() as session:
            session.store(User("Idan"), key="users/1-A")
            session.store(User("Idan"), key="users/2-A")
            session.save_changes()

        all_observer = self.store.changes().for_all_time_series()
        all_observer.subscribe(ActionObserver(on_next=on_next))
        all_observer.ensure_subscribe_now()

        with self.store.open_session() as session:
            tsf = session.time_series_for("users/1-A", "Heartrate")
            tsf.append(datetime.now(), values=86)
            tsf = session.time_series_for("users/2-A", "Likes")
            tsf.append(datetime.now(), values=567)

            session.save_changes()

        event.wait(1)
        self.assertEqual(len(changes), 2)

    def test_for_time_series_of_document(self):
        changes = []

        observer = self.store.changes().for_time_series_of_document("users/1-A")
        observer.subscribe(changes.append)
        observer.ensure_subscribe_now()

        with self.store.open_session() as session:
            session.store(User("Idan"), key="users/1-A")
            session.store(User("Idan"), key="users/2-A")
            session.save_changes()

        with self.store.open_session() as session:
            tsf = session.time_series_for("users/1-A", "Likes")
            tsf.append(datetime.now(), values=33)
            tsf = session.time_series_for("users/1-A", "Heartrate")
            tsf.append(datetime.now(), values=76)
            tsf = session.time_series_for("users/2-A", "Heartrate")
            tsf.append(datetime.now(), values=67)
            session.save_changes()

        sleep(1)
        self.assertTrue(len(changes) == 2)
        self.assertEqual("users/1-A", changes[0]["DocumentId"])

    def test_for_time_series_of_document_with_time_series_name(self):
        changes = []

        observer = self.store.changes().for_time_series_of_document("users/1-A", time_series_name="Heartrate")
        observer.subscribe(changes.append)
        observer.ensure_subscribe_now()

        with self.store.open_session() as session:
            session.store(User("Idan"), key="users/1-A")
            session.save_changes()

        with self.store.open_session() as session:
            tsf = session.time_series_for("users/1-A", "Likes")
            tsf.append(datetime.now(), values=33)
            tsf2 = session.time_series_for("users/1-A", "Heartrate")
            tsf2.append(datetime.now(), values=76)
            session.save_changes()

        sleep(1)
        self.assertTrue(len(changes) == 1)
        self.assertEqual("users/1-A", changes[0]["DocumentId"])
        self.assertEqual('Put', changes[0]["Type"])
        self.assertEqual("Heartrate", changes[0]["Name"])

    def test_for_time_series(self):
        changes = []

        observer = self.store.changes().for_time_series("Likes")
        observer.subscribe(changes.append)
        observer.ensure_subscribe_now()

        with self.store.open_session() as session:
            session.store(User("Idan"), key="users/1-A")
            session.store(User("Idan"), key="users/2-A")
            session.save_changes()

        with self.store.open_session() as session:
            tsf = session.time_series_for("users/1-A", "Likes")
            tsf.append(datetime.now(), values=33)
            tsf = session.time_series_for("users/1-A", "Heartrate")
            tsf.append(datetime.now(), values=76)
            tsf = session.time_series_for("users/2-A", "Likes")
            tsf.append(datetime.now(), values=67)
            session.save_changes()

        sleep(1)
        self.assertTrue(len(changes) == 2)

    def test_for_all_counters(self):
        event = Event()
        changes = []

        def on_next(value):
            changes.append(value)
            if len(changes) == 2:
                event.set()

        with self.store.open_session() as session:
            session.store(User("Idan"), key="users/1-A")
            session.store(User("Idan"), key="users/2-A")
            session.save_changes()

        all_observer = self.store.changes().for_all_counters()
        all_observer.subscribe(ActionObserver(on_next=on_next))
        all_observer.ensure_subscribe_now()

        with self.store.open_session() as session:
            document_counter = session.counters_for("users/1-A")
            document_counter.increment("Likes", delta=10)
            document_counter = session.counters_for("users/2-A")
            document_counter.increment("Shares", delta=200)

            session.save_changes()

        event.wait(1)
        self.assertEqual(len(changes), 2)

    def test_for_counters_of_document(self):
        changes = []

        observer = self.store.changes().for_counters_of_document("users/1-A")
        observer.subscribe(changes.append)
        observer.ensure_subscribe_now()

        with self.store.open_session() as session:
            session.store(User("Idan"), key="users/1-A")
            session.store(User("Idan"), key="users/2-A")
            session.save_changes()

        with self.store.open_session() as session:
            document_counter = session.counters_for("users/1-A")
            document_counter.increment("Likes", delta=10)
            document_counter.increment("Shares", delta=100)
            document_counter = session.counters_for("users/2-A")
            document_counter.increment("Shares", delta=200)

            session.save_changes()

        sleep(1)
        self.assertTrue(len(changes) == 2)
        self.assertTrue(all("users/1-A" == change["DocumentId"] for change in changes))

    def test_for_counter_of_document(self):
        changes = []

        observer = self.store.changes().for_counter_of_document("users/1-A", counter_name="Shares")
        observer.subscribe(changes.append)
        observer.ensure_subscribe_now()

        with self.store.open_session() as session:
            session.store(User("Idan"), key="users/1-A")
            session.save_changes()

        with self.store.open_session() as session:
            document_counter = session.counters_for("users/1-A")
            document_counter.increment("Likes", delta=10)
            document_counter.increment("Shares", delta=100)
            session.save_changes()

        sleep(1)
        self.assertTrue(len(changes) == 1)
        self.assertEqual("users/1-A", changes[0]["DocumentId"])
        self.assertEqual('Put', changes[0]["Type"])
        self.assertEqual("Shares", changes[0]["Name"])

    def test_for_counter(self):
        changes = []

        observer = self.store.changes().for_counter("Likes")
        observer.subscribe(changes.append)
        observer.ensure_subscribe_now()

        with self.store.open_session() as session:
            session.store(User("Idan"), key="users/1-A")
            session.store(User("Idan"), key="users/2-A")
            session.save_changes()

        with self.store.open_session() as session:
            document_counter_1 = session.counters_for("users/1-A")
            document_counter_1.increment("Likes", delta=10)
            document_counter_1.increment("Shares", delta=100)
            document_counter_2 = session.counters_for("users/2-A")
            document_counter_2.increment("shares", delta=200)
            document_counter_2.increment("Likes", delta=10)
            session.save_changes()

        with self.store.open_session() as session:
            document_counter_1 = session.counters_for("users/1-A")
            document_counter_1.increment("Likes", delta=18)
            session.save_changes()

        sleep(1)
        check_likes_changed = {"name": 0, "put": 0, "increment": 0}
        self.assertTrue(len(changes) == 3)
        for change in changes:
            if change["DocumentId"] == "users/1-A":
                self.assertTrue(change["Type"] in ("Increment", "Put"))
            else:
                self.assertEqual("Put", change["Type"])
            if change["Name"] == "Likes":
                check_likes_changed["name"] += 1
            if change["Type"] == "Put":
                check_likes_changed['put'] += 1
            elif change["Type"] == "Increment":
                check_likes_changed["increment"] += 1

        self.assertTrue(check_likes_changed["name"] == 3 and check_likes_changed["put"] == 2 and
                        check_likes_changed["increment"] == 1)


if __name__ == "__main__":
    unittest.main()

from queue import Queue

from ravendb.changes.types import CounterChange, CounterChangeTypes
from ravendb.infrastructure.entities import User
from ravendb.tests.test_base import TestBase


class TestRavenDB11703(TestBase):
    def setUp(self):
        super(TestRavenDB11703, self).setUp()

    def test_can_get_notification_about_counter_increment(self):
        changes_queue = Queue()
        observer = self.store.changes().for_counters_of_document("users/1")
        close_method = observer.subscribe(changes_queue.put)

        with self.store.open_session() as session:
            session.store(User(), "users/1")
            session.save_changes()

        with self.store.open_session() as session:
            session.counters_for("users/1").increment("likes")
            session.save_changes()

        counter_change: CounterChange = changes_queue.get(timeout=2)
        self.assertIsNotNone(counter_change)

        self.assertEqual("users/1", counter_change.document_id)
        self.assertEqual("Users", counter_change.collection_name)
        self.assertEqual(CounterChangeTypes.PUT, counter_change.type_of_change)
        self.assertEqual("likes", counter_change.name)
        self.assertEqual(1, counter_change.value)
        self.assertIsNotNone(counter_change.change_vector)

        with self.store.open_session() as session:
            session.counters_for("users/1").increment("likes")
            session.save_changes()

        counter_change: CounterChange = changes_queue.get(timeout=2)
        self.assertIsNotNone(counter_change)

        self.assertEqual("users/1", counter_change.document_id)
        self.assertEqual("Users", counter_change.collection_name)
        self.assertEqual(CounterChangeTypes.INCREMENT, counter_change.type_of_change)
        self.assertEqual("likes", counter_change.name)
        self.assertEqual(2, counter_change.value)
        self.assertIsNotNone(counter_change.change_vector)

        close_method()

    def test_can_get_notification_about_counter_delete(self):
        changes_queue = Queue()
        observer = self.store.changes().for_counters_of_document("users/1")
        close_method = observer.subscribe(changes_queue.put)

        with self.store.open_session() as session:
            session.store(User(), "users/1")
            session.save_changes()

        with self.store.open_session() as session:
            session.counters_for("users/1").increment("likes")
            session.save_changes()

        counter_change: CounterChange = changes_queue.get(timeout=20)
        self.assertIsNotNone(counter_change)

        self.assertEqual("users/1", counter_change.document_id)
        self.assertEqual("Users", counter_change.collection_name)
        self.assertEqual(CounterChangeTypes.PUT, counter_change.type_of_change)
        self.assertEqual("likes", counter_change.name)
        self.assertEqual(1, counter_change.value)
        self.assertIsNotNone(counter_change.change_vector)

        with self.store.open_session() as session:
            session.counters_for("users/1").delete("likes")
            session.save_changes()

        counter_change = changes_queue.get(timeout=20)
        self.assertIsNotNone(counter_change)

        self.assertEqual("users/1", counter_change.document_id)
        self.assertEqual(CounterChangeTypes.DELETE, counter_change.type_of_change)
        self.assertEqual("likes", counter_change.name)
        self.assertEqual(0, counter_change.value)
        self.assertIsNotNone(counter_change.change_vector)
        close_method()

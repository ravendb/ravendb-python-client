from threading import Event
from typing import Optional, List

from ravendb import AbstractIndexCreationTask, SetIndexesPriorityOperation
from ravendb.changes.observers import ActionObserver
from ravendb.changes.types import DocumentChange, IndexChange
from ravendb.documents.indexes.definitions import IndexPriority
from ravendb.infrastructure.entities import User
from ravendb.infrastructure.orders import Order
from ravendb.tests.test_base import TestBase


class UsersByName(AbstractIndexCreationTask):
    def __init__(self):
        super(UsersByName, self).__init__()

        self.map = "from c in docs.Users select new { c.name, count = 1 }"

        self.reduce = (
            "from result in results "
            + "group result by result.name "
            + "into g "
            + "select new "
            + "{ "
            + "  name = g.Key, "
            + "  count = g.Sum(x => x.count) "
            + "}"
        )


class TestChanges(TestBase):
    def setUp(self):
        super(TestChanges, self).setUp()

    def test_all_documents_changes(self):
        event = Event()
        document_change: Optional[DocumentChange] = None
        observable = self.store.changes().for_all_documents()

        def __ev(value: DocumentChange):
            nonlocal document_change
            document_change = value
            event.set()

        subscription = ActionObserver(__ev)
        close_action = observable.subscribe(subscription)
        observable.ensure_subscribe_now()

        with self.store.open_session() as session:
            session.store(User("users/1"))
            session.save_changes()

        event.wait(1)
        self.assertIsNotNone(document_change)
        self.assertEqual("users/1", document_change.key)
        self.assertEqual("Put", document_change.type_of_change.value)
        document_change = None
        event.clear()

        event.wait(1)
        self.assertIsNone(document_change)

        close_action()

        with self.store.open_session() as session:
            user = User(name="another name")
            session.store(user, "users/1")
            session.save_changes()

        event.wait(1)
        self.assertIsNone(document_change)

    def test_can_can_notification_about_documents_from_collection(self):
        event = Event()
        changes_list: List[DocumentChange] = []
        observable = self.store.changes().for_documents_in_collection("users")

        def __ev(value: DocumentChange):
            nonlocal changes_list
            changes_list.append(value)
            if len(changes_list) == 2:
                event.set()

        subscription = ActionObserver(__ev)
        close_action = observable.subscribe(subscription)
        observable.ensure_subscribe_now()

        with self.store.open_session() as session:
            session.store(User("users/1"))
            session.save_changes()

        with self.store.open_session() as session:
            session.store(Order("orders/1"))
            session.save_changes()

        with self.store.open_session() as session:
            session.store(User("users/2"))
            session.save_changes()

        event.wait(2)

        self.assertIsNotNone(changes_list[0])
        self.assertEqual(changes_list[0].key, "users/1")

        self.assertIsNotNone(changes_list[1])
        self.assertEqual(changes_list[1].key, "users/2")

        close_action()

    def test_single_document_changes(self):
        event = Event()
        document_change: Optional[DocumentChange] = None
        observable = self.store.changes().for_document("users/1")

        def __ev(value: DocumentChange):
            nonlocal document_change
            document_change = value
            event.set()

        subscription = ActionObserver(__ev)
        close_action = observable.subscribe(subscription)
        observable.ensure_subscribe_now()

        with self.store.open_session() as session:
            session.store(User("users/1"))
            session.save_changes()

        event.wait(1)
        self.assertIsNotNone(document_change)
        self.assertEqual("users/1", document_change.key)
        self.assertEqual("Users", document_change.collection_name)
        self.assertEqual("Put", document_change.type_of_change.value)

        document_change = None
        event.clear()

        event.wait(1)
        self.assertIsNone(document_change)

        close_action()

        with self.store.open_session() as session:
            user = User(name="another name")
            session.store(user, "users/1")
            session.save_changes()

        event.wait(1)
        self.assertIsNone(document_change)

    def test_single_index_changes(self):
        index = UsersByName()
        self.store.execute_index(index)
        event = Event()
        index_change: Optional[IndexChange] = None

        observable = self.store.changes().for_index(index.index_name)

        def __ev(value: IndexChange):
            nonlocal index_change
            index_change = value
            event.set()

        subscription = ActionObserver(__ev)
        close_action = observable.subscribe(subscription)
        observable.ensure_subscribe_now()

        operation = SetIndexesPriorityOperation(IndexPriority.LOW, index.index_name)
        self.store.maintenance.send(operation)
        event.wait(1)
        self.assertIsNotNone(index_change)
        self.assertEqual(index.index_name, index_change.name)
        close_action()

    def test_all_index_changes(self):
        index = UsersByName()
        self.store.execute_index(index)
        event = Event()
        index_change: Optional[IndexChange] = None

        observable = self.store.changes().for_all_indexes()

        def __ev(value: IndexChange):
            nonlocal index_change
            index_change = value
            event.set()

        subscription = ActionObserver(__ev)
        close_action = observable.subscribe(subscription)
        observable.ensure_subscribe_now()

        operation = SetIndexesPriorityOperation(IndexPriority.LOW, index.index_name)
        self.store.maintenance.send(operation)

        event.wait(1)
        self.assertIsNotNone(index_change)
        self.assertEqual(index.index_name, index_change.name)
        close_action()

    def test_can_notification_about_documents_starting_with(self):
        event = Event()
        document_changes: List[DocumentChange] = []

        observable = self.store.changes().for_documents_start_with("users/")

        def __ev(value: DocumentChange):
            nonlocal document_changes
            document_changes.append(value)
            if len(document_changes) == 2:
                event.set()

        subscription = ActionObserver(__ev)
        close_action = observable.subscribe(subscription)
        observable.ensure_subscribe_now()

        with self.store.open_session() as session:
            session.store(User(), "users/1")
            session.save_changes()

        with self.store.open_session() as session:
            session.store(User(), "differentDocumentPrefix/1")
            session.save_changes()

        with self.store.open_session() as session:
            session.store(User(), "users/2")
            session.save_changes()

            event.wait(1)

        self.assertEqual(2, len(document_changes))

        self.assertIsNotNone(document_changes[0])
        self.assertEqual("users/1", document_changes[0].key)

        self.assertIsNotNone(document_changes[1])
        self.assertEqual("users/2", document_changes[1].key)

        close_action()

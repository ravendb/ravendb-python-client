from threading import Event
from typing import Optional

from ravendb.changes.observers import ActionObserver
from ravendb.changes.types import DocumentChange
from ravendb.infrastructure.entities import User
from ravendb.tests.test_base import TestBase


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

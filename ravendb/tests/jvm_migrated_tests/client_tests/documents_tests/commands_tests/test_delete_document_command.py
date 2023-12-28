import unittest

from ravendb import DeleteDocumentCommand
from ravendb.exceptions.raven_exceptions import ConcurrencyException
from ravendb.infrastructure.entities import User
from ravendb.tests.test_base import TestBase


class TestDeleteDocumentCommand(TestBase):
    def setUp(self):
        super(TestDeleteDocumentCommand, self).setUp()

    def test_can_delete_document(self):
        with self.store.open_session() as session:
            user = User(name="Marcin")
            session.store(user, "users/1")
            session.save_changes()

        command = DeleteDocumentCommand("users/1")
        self.store.get_request_executor().execute_command(command)

        with self.store.open_session() as session:
            loaded_user = session.load("users/1", User)
            self.assertIsNone(loaded_user)

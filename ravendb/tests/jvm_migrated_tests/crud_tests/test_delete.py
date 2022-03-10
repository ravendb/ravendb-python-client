from ravendb.tests.test_base import TestBase, User


class TestDelete(TestBase):
    def setUp(self):
        super(TestDelete, self).setUp()

    def test_delete_document_by_entity(self):
        with self.store.open_session() as session:
            user = User("RavenDB", None)
            session.store(user, "users/1")
            session.save_changes()

            user = session.load("users/1", User)
            self.assertIsNotNone(user)
            session.delete(user)
            session.save_changes()

            null_user = session.load("users/1", User)
            self.assertIsNone(null_user)

    def test_delete_document_by_id(self):
        with self.store.open_session() as session:
            user = User("RavenDB", None)
            session.store(user, "users/1")
            session.save_changes()

            user = session.load("users/1", User)
            self.assertIsNotNone(user)
            session.delete("users/1")
            session.save_changes()

            null_user = session.load("users/1", User)
            self.assertIsNone(null_user)

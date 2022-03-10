from ravendb.tests.test_base import TestBase, User


class TestExists(TestBase):
    def setUp(self):
        super(TestExists, self).setUp()

    def test_check_if_document_exists(self):
        with self.store.open_session() as session:
            user1 = User("Jacob", None)
            user2 = User("Bacon", None)
            session.store(user1, "users/1")
            session.store(user2, "users/2")
            session.save_changes()

        with self.store.open_session() as session:
            self.assertFalse(session.advanced.exists("users/10"))
            self.assertTrue(session.advanced.exists("users/1"))
            session.load("users/2", User)
            self.assertTrue(session.advanced.exists("users/2"))

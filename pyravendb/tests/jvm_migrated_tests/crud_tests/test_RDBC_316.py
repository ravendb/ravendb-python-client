from pyravendb.tests.test_base import TestBase, UserWithId


class TestRDBC316(TestBase):
    def setUp(self):
        super(TestRDBC316, self).setUp()

    def test_can_store_equal_document_under_two_different_keys(self):
        with self.store.open_session() as session:
            user1 = UserWithId("Marcin")
            user2 = UserWithId("Marcin")
            self.assertDictEqual(user1.__dict__, user2.__dict__)
            session.store(user1, "users/1")
            session.store(user2, "users/2")
            session.save_changes()

        with self.store.open_session() as session:
            user1 = session.load("users/1", UserWithId)
            user2 = session.load("users/2", UserWithId)
            self.assertIsNotNone(user1)
            self.assertIsNotNone(user2)
            self.assertFalse(user1 == user2)

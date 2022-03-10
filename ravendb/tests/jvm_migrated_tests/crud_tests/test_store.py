from ravendb.tests.test_base import TestBase, User


class TestStore(TestBase):
    def setUp(self):
        super(TestStore, self).setUp()

    def test_store_document(self):
        with self.store.open_session() as session:
            user = User("Jacex", 10)
            session.store(user, "users/1")
            session.save_changes()

        with self.store.open_session() as session:
            user = session.load("users/1", User)
            self.assertIsNotNone(user)
            self.assertEqual(user.name, "Jacex")

    def test_store_documents(self):
        with self.store.open_session() as session:
            user1 = User("Jacex", 10)
            user2 = User("Jacob", 20)
            session.store(user1, "users/1")
            session.store(user2, "users/2")
            session.save_changes()
            self.assertEqual(len(session.load(["users/1", "users/2"], User)), 2)

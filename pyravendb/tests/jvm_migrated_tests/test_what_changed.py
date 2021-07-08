from pyravendb.tests.test_base import TestBase, User


class TestWhatChanged(TestBase):

    def setUp(self):
        super(TestWhatChanged, self).setUp()

    def test_has_changes(self):
        with self.store.open_session() as session:
            user1 = User("user1", None)
            user2 = User("user2", 1)

            session.store(user1, "users/1")
            session.store(user2, "users/2")
            session.save_changes()

        with self.store.open_session() as session:
            self.assertFalse(session.advanced.has_changes())

            user1, user2 = session.load(["users/1", "users/2"], User)

            self.assertFalse(session.advanced.has_changed(user1))
            self.assertFalse(session.advanced.has_changed(user2))

            user1.name = "newName"

            self.assertTrue(session.advanced.has_changed(user1))
            self.assertFalse(session.advanced.has_changed(user2))
            self.assertTrue(session.advanced.has_changes())


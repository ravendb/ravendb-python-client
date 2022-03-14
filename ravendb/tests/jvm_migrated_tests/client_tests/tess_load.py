from ravendb.infrastructure.entities import User
from ravendb.tests.test_base import TestBase


class LoadTest(TestBase):
    def setUp(self):
        super(LoadTest, self).setUp()

    def test_load_document_and_expect_null_user(self):
        with self.store.open_session() as session:
            null_id = None
            user1 = session.load(null_id, User)
            self.assertIsNone(user1)

            user2 = session.load("", User)
            self.assertIsNone(user2)

            user3 = session.load(" ", User)
            self.assertIsNone(user3)

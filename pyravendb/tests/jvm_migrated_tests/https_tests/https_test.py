from pyravendb.tests.test_base import TestBase, User


class HttpsTest(TestBase):
    def setUp(self):
        super(HttpsTest, self).setUp()

    def test_can_connect_with_certificate(self):
        with self.secured_document_store as store:
            with store.open_session() as session:
                user = User("user1")
                session.store(user, "users/1")
                session.save_changes()

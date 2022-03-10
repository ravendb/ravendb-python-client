from ravendb.documents.session.misc import SessionOptions, TransactionMode
from ravendb.infrastructure.entities import User
from ravendb.tests.test_base import TestBase


class TestClusterTransaction(TestBase):
    def setUp(self):
        super().setUp()

    def test_can_create_cluster_transaction_request(self):
        user1 = User()
        user1.name = "Karmel"

        user3 = User()
        user3.name = "Indych"

        session_options = SessionOptions()
        session_options.transaction_mode = TransactionMode.CLUSTER_WIDE
        session_options.disable_atomic_document_writes_in_cluster_wide_transaction = True

        with self.store.open_session(session_options=session_options) as session:
            session.advanced.cluster_transaction.create_compare_exchange_value("usernames/ayende", user1)
            session.store(user3, "foo/bar")
            session.save_changes()

            user = session.advanced.cluster_transaction.get_compare_exchange_value("usernames/ayende", User).value
            self.assertEqual(user1.name, user.name)
            user = session.load("foo/bar", User)
            self.assertEqual(user3.name, user.name)

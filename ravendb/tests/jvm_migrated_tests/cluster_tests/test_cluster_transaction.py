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

    def test_throw_on_invalid_transaction_mode(self):
        user1 = User()
        user1.name = "Karmel"

        with self.store.open_session() as session:
            with self.assertRaises(RuntimeError):
                session.advanced.cluster_transaction.create_compare_exchange_value("usernames/ayende", user1)
            with self.assertRaises(RuntimeError):
                session.advanced.cluster_transaction.delete_compare_exchange_value("usernames/ayende", 0)

        options = SessionOptions(transaction_mode=TransactionMode.CLUSTER_WIDE)
        options.disable_atomic_document_writes_in_cluster_wide_transaction = True

        with self.store.open_session(session_options=options) as session:
            session.advanced.cluster_transaction.create_compare_exchange_value("usernames/ayende", user1)
            session.advanced.transaction_mode = TransactionMode.SINGLE_NODE
            with self.assertRaises(RuntimeError):
                session.save_changes()

            session.advanced.transaction_mode = TransactionMode.CLUSTER_WIDE
            session.save_changes()

            u = session.advanced.cluster_transaction.get_compare_exchange_value("usernames/ayende", User)
            self.assertEqual(user1.name, u.value.name)

    def test_session_sequence(self):
        user1 = User(name="Karmel")
        user2 = User(name="Indych")

        session_options = SessionOptions(
            transaction_mode=TransactionMode.CLUSTER_WIDE,
            disable_atomic_document_writes_in_cluster_wide_transaction=True,
        )

        with self.store.open_session(session_options=session_options) as session:
            session.advanced.cluster_transaction.create_compare_exchange_value("usernames/ayende", user1)
            session.store(user1, "users/1")
            session.save_changes()

            value = session.advanced.cluster_transaction.get_compare_exchange_value("usernames/ayende", User)
            value.value = user2

            session.store(user2, "users/2")
            user1.age = 10
            session.store(user1, "users/1")
            session.save_changes()

import unittest

from ravendb.documents.commands.batches import CommandType, CountersBatchCommandData
from ravendb.documents.operations.counters import CounterOperation, CounterOperationType
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

    def test_throw_on_unsupported_operations(self):
        session_options = SessionOptions(
            transaction_mode=TransactionMode.CLUSTER_WIDE,
            disable_atomic_document_writes_in_cluster_wide_transaction=True,
        )

        with self.store.open_session(session_options=session_options) as session:
            from ravendb.documents.session.document_session_operations.in_memory_document_session_operations import (
                InMemoryDocumentSessionOperations,
            )

            counter_op = CounterOperation("likes", CounterOperationType.INCREMENT, 1)
            counter_cmd = CountersBatchCommandData("docs/1", counter_op)

            save_changes_data = InMemoryDocumentSessionOperations.SaveChangesData(session)
            save_changes_data.session_commands.append(counter_cmd)

            with self.assertRaises(ValueError) as ctx:
                session.validate_cluster_transaction(save_changes_data)

            self.assertIn("not supported", str(ctx.exception))

    def test_compare_exchange_double_create_raises(self):
        session_options = SessionOptions(
            transaction_mode=TransactionMode.CLUSTER_WIDE,
            disable_atomic_document_writes_in_cluster_wide_transaction=True,
        )

        with self.store.open_session(session_options=session_options) as session:
            session.advanced.cluster_transaction.create_compare_exchange_value("users/emails/john", "john@doe.com")

            with self.assertRaises(RuntimeError):
                session.advanced.cluster_transaction.create_compare_exchange_value("users/emails/john", "other@doe.com")


class TestClusterTransactionValidation(unittest.TestCase):
    def test_cluster_tx_rejects_unsupported_command_types(self):
        import inspect
        from ravendb.documents.session.document_session_operations.in_memory_document_session_operations import (
            InMemoryDocumentSessionOperations,
        )

        src = inspect.getsource(InMemoryDocumentSessionOperations.validate_cluster_transaction)
        self.assertNotIn(
            "== CommandType.PUT or CommandType.DELETE",
            src,
            "Cluster TX validation uses 'x == A or B' (always True). Must use 'x in (A, B)'.",
        )

    def test_compare_exchange_rejects_double_create(self):
        from ravendb.documents.operations.compare_exchange.compare_exchange import (
            CompareExchangeSessionValue,
            CompareExchangeValueState,
        )

        value = CompareExchangeSessionValue.__new__(CompareExchangeSessionValue)
        value._key = "test"
        value._state = CompareExchangeValueState.CREATED
        with self.assertRaises(RuntimeError):
            value._CompareExchangeSessionValue__assert_state()

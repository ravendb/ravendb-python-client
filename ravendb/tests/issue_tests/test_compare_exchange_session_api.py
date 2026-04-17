from ravendb import SessionOptions, TransactionMode
from ravendb.tests.test_base import TestBase, User


class TestCompareExchangeSessionApi(TestBase):
    def setUp(self):
        super().setUp()

    def test_get_compare_exchange_value_returns_metadata(self):
        key = "locks/happy-metadata"
        options = SessionOptions(transaction_mode=TransactionMode.CLUSTER_WIDE)

        with self.store.open_session(session_options=options) as session:
            value = session.advanced.cluster_transaction.create_compare_exchange_value(key, User(name="Simon"))
            value.metadata["@expires"] = "2099-01-01T00:00:00.0000000Z"
            value.metadata["@created-at"] = "2026-04-15T12:27:51.556568Z"
            value.metadata["Custom-Tag"] = "hello"
            session.save_changes()

        with self.store.open_session(session_options=options) as session:
            got = session.advanced.cluster_transaction.get_compare_exchange_value(key, User)

            self.assertIsNotNone(got)
            self.assertEqual("Simon", got.value.name)
            self.assertTrue(got.has_metadata)
            self.assertEqual("2099-01-01T00:00:00.0000000Z", got.metadata["@expires"])
            self.assertEqual("2026-04-15T12:27:51.556568Z", got.metadata["@created-at"])
            self.assertEqual("hello", got.metadata["Custom-Tag"])

    def test_delete_compare_exchange_value_removes_item(self):
        key = "locks/happy-delete"
        options = SessionOptions(transaction_mode=TransactionMode.CLUSTER_WIDE)

        with self.store.open_session(session_options=options) as session:
            session.advanced.cluster_transaction.create_compare_exchange_value(key, User(name="Simon"))
            session.save_changes()

        with self.store.open_session(session_options=options) as session:
            current = session.advanced.cluster_transaction.get_compare_exchange_value(key, User)
            self.assertIsNotNone(current)

            session.advanced.cluster_transaction.delete_compare_exchange_value(current)
            session.save_changes()

        with self.store.open_session(session_options=options) as session:
            after = session.advanced.cluster_transaction.get_compare_exchange_value(key, User)
            self.assertIsNone(after)

    def test_delete_compare_exchange_value_by_key_and_index_removes_item(self):
        key = "locks/happy-delete-by-key"
        options = SessionOptions(transaction_mode=TransactionMode.CLUSTER_WIDE)

        with self.store.open_session(session_options=options) as session:
            session.advanced.cluster_transaction.create_compare_exchange_value(key, User(name="Simon"))
            session.save_changes()

        with self.store.open_session(session_options=options) as session:
            current = session.advanced.cluster_transaction.get_compare_exchange_value(key, User)
            self.assertIsNotNone(current)

            session.advanced.cluster_transaction.delete_compare_exchange_value(current.key, current.index)
            session.save_changes()

        with self.store.open_session(session_options=options) as session:
            after = session.advanced.cluster_transaction.get_compare_exchange_value(key, User)
            self.assertIsNone(after)

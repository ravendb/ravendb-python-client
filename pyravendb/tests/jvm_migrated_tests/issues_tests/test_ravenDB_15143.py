import datetime
from typing import Optional

from pyravendb.documents.session.loaders.include import QueryIncludeBuilder
from pyravendb.documents.session.misc import SessionOptions, TransactionMode
from pyravendb.tests.test_base import TestBase


class Locker:
    def __init__(self, client_id: Optional[str] = None):
        self.client_id = client_id


class Command:
    def __init__(self, Id: Optional[str] = None):
        self.Id = Id


class TestRavenDB15143(TestBase):
    def setUp(self):
        super().setUp()

    def test_can_use_create_cmp_xng_to_in_session_with_no_other_changes(self):
        session_options = SessionOptions(transaction_mode=TransactionMode.CLUSTER_WIDE)

        with self.store.open_session(session_options=session_options) as session:
            session.store(Command("cmd/239-A"))
            session.save_changes()

        def __include_id(builder: QueryIncludeBuilder):
            builder.include_compare_exchange_value("key")

        with self.store.open_session(session_options=session_options) as session:
            result = session.query(object_type=Command).include(__include_id).first()

            self.assertIsNotNone(result)
            locker = session.advanced.cluster_transaction.get_compare_exchange_value("cmd/239-A", Locker)
            self.assertIsNone(locker)

            locker_object = Locker()
            locker_object.client_id = "a"
            locker = session.advanced.cluster_transaction.create_compare_exchange_value("cmd/239-A", locker_object)

            locker.metadata["@expires"] = datetime.datetime.now() + datetime.timedelta(minutes=2)
            session.save_changes()

        with self.store.open_session(session_options=session_options) as session:
            smile = session.advanced.cluster_transaction.get_compare_exchange_value("cmd/239-A", str)
            self.assertIsNotNone(smile)

    def test_can_store_and_read_primitive_compare_exchange(self):
        session_options = SessionOptions(transaction_mode=TransactionMode.CLUSTER_WIDE)
        with self.store.open_session(session_options=session_options) as session:
            session.advanced.cluster_transaction.create_compare_exchange_value("cmd/int", 5)
            session.advanced.cluster_transaction.create_compare_exchange_value("cmd/string", "testing")
            session.advanced.cluster_transaction.create_compare_exchange_value("cmd/true", True)
            session.advanced.cluster_transaction.create_compare_exchange_value("cmd/false", False)
            session.advanced.cluster_transaction.create_compare_exchange_value("cmd/zero", 0)
            session.advanced.cluster_transaction.create_compare_exchange_value("cmd/null", None)

            session.save_changes()

        with self.store.open_session(session_options=session_options) as session:
            number_value = session.advanced.cluster_transaction.get_compare_exchange_value("cmd/int", int)
            self.assertEqual(5, number_value.value)
            string_value = session.advanced.cluster_transaction.get_compare_exchange_value("cmd/string", str)
            self.assertEqual("testing", string_value.value)
            true_value = session.advanced.cluster_transaction.get_compare_exchange_value("cmd/true", bool)
            self.assertTrue(true_value.value)
            false_value = session.advanced.cluster_transaction.get_compare_exchange_value("cmd/false", bool)
            self.assertFalse(false_value.value)
            zero_value = session.advanced.cluster_transaction.get_compare_exchange_value("cmd/zero", int)
            self.assertEqual(0, zero_value.value)
            null_value = session.advanced.cluster_transaction.get_compare_exchange_value("cmd/null", int)
            self.assertIsNone(null_value.value)

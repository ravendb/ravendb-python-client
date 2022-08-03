import unittest

from ravendb import SessionOptions, TransactionMode
from ravendb.infrastructure.entities import User
from ravendb.tests.test_base import TestBase


class TestRavenDB14989(TestBase):
    def setUp(self):
        super(TestRavenDB14989, self).setUp()

    @unittest.skip("Exception dispatcher")
    def test_should_work(self):
        session_options = SessionOptions(transaction_mode=TransactionMode.CLUSTER_WIDE)

        with self.store.open_session(session_options=session_options) as session:
            user = User(name="Egor")
            session.advanced.cluster_transaction.create_compare_exchange_value("e" * 513, user)
            with self.assertRaises(CompareExchangeKeyTooBigException):
                session.save_changes()

from ravendb import SessionOptions, TransactionMode
from ravendb.documents.operations.compare_exchange.operations import (
    PutCompareExchangeValueOperation,
    CompareExchangeResult,
)
from ravendb.infrastructure.entities import User
from ravendb.infrastructure.orders import Employee, Company, Address
from ravendb.tests.test_base import TestBase, UserWithId


class TestRavenDB12132(TestBase):
    def setUp(self):
        super().setUp()

    def test_can_put_object_with_id(self):
        user = UserWithId("Grisha", None, "users/1")
        res: CompareExchangeResult = self.store.operations.send(PutCompareExchangeValueOperation("test", user, 0))
        self.assertTrue(res.successful)
        self.assertEqual("Grisha", res.value.name)
        self.assertEqual("users/1", res.value.Id)
        # todo: discuss creating class with "Id" inside and with init
        # todo: class parser needs the same variable names for both __init__ argument and field
        # todo: so we can't do something like __init__(self, identifier): \n self.Id = identifier

    def test_can_create_cluster_transaction_request_1(self):
        user = User()
        user.Id = "this/is/my/id"
        user.name = "Grisha"

        session_options = SessionOptions(transaction_mode=TransactionMode.CLUSTER_WIDE)

        with self.store.open_session(session_options=session_options) as session:
            session.advanced.cluster_transaction.create_compare_exchange_value("usernames/ayende", user)
            session.save_changes()

            user_from_cluster = session.advanced.cluster_transaction.get_compare_exchange_value(
                "usernames/ayende", User
            ).value
            self.assertEqual(user.name, user_from_cluster.name)
            self.assertEqual(user.Id, user_from_cluster.Id)

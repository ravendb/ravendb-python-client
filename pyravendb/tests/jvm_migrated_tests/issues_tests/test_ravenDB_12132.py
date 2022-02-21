from pyravendb.documents.operations.compare_exchange.operations import (
    PutCompareExchangeValueOperation,
    CompareExchangeResult,
)
from pyravendb.tests.test_base import TestBase, UserWithId


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

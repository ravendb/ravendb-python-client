from typing import Dict

from ravendb.documents.store.definition import SessionOptions
from ravendb.documents.operations.compare_exchange.operations import (
    PutCompareExchangeValueOperation,
    CompareExchangeResult,
    CompareExchangeValue,
    GetCompareExchangeValueOperation,
    DeleteCompareExchangeValueOperation,
    GetCompareExchangeValuesOperation,
)
from ravendb.documents.operations.statistics import GetDetailedStatisticsOperation, DetailedDatabaseStatistics
from ravendb.documents.session.misc import TransactionMode
from ravendb.tests.test_base import TestBase, User
from ravendb.util.util import StartingWithOptions


class TestUniqueValues(TestBase):
    def setUp(self):
        super().setUp()

    def test_can_put_multi_different_values(self):
        user1 = User("Karmel")
        res: CompareExchangeResult[User] = self.store.operations.send(
            PutCompareExchangeValueOperation[User]("test", user1, 0)
        )

        user2 = User("Karmel")
        res2: CompareExchangeResult[User] = self.store.operations.send(
            PutCompareExchangeValueOperation("test2", user2, 0)
        )

        self.assertEqual("Karmel", res.value.name)
        self.assertTrue(res.successful)
        self.assertEqual("Karmel", res2.value.name)
        self.assertTrue(res2.successful)

    def test_can_put_unique_string(self):
        self.store.operations.send(PutCompareExchangeValueOperation("test", "Karmel", 0))
        res: CompareExchangeValue[str] = self.store.operations.send(GetCompareExchangeValueOperation[str]("test", str))
        self.assertEqual("Karmel", res.value)

    def test_can_work_with_primitive_types(self):
        res = self.store.operations.send(GetCompareExchangeValueOperation("test", int))
        self.assertIsNone(res)

        self.store.operations.send(PutCompareExchangeValueOperation("test", 5, 0))

        res = self.store.operations.send(GetCompareExchangeValueOperation("test", int))

        self.assertIsNotNone(res)
        self.assertEqual(5, res.value)

    def test_can_read_non_existing_key(self):
        res = self.store.operations.send(GetCompareExchangeValueOperation("test", int))
        self.assertIsNone(res)

    def test_can_get_index_value(self):
        user = User("Karmel")

        self.store.operations.send(PutCompareExchangeValueOperation("test", user, 0))
        res: CompareExchangeValue[User] = self.store.operations.send(GetCompareExchangeValueOperation("test", User))

        self.assertEqual("Karmel", res.value.name)

        user2 = User("Karmel2")

        res2: CompareExchangeResult[User] = self.store.operations.send(
            PutCompareExchangeValueOperation("test", user2, res.index)
        )
        self.assertTrue(res2.successful)
        self.assertEqual("Karmel2", res2.value.name)

    def test_can_remove_unique(self):
        res: CompareExchangeResult[str] = self.store.operations.send(
            PutCompareExchangeValueOperation[str]("test", "Karmel", 0)
        )
        self.assertEqual("Karmel", res.value)
        self.assertTrue(res.successful)

        res: CompareExchangeResult[str] = self.store.operations.send(
            DeleteCompareExchangeValueOperation[str](str, "test", res.index)
        )
        self.assertTrue(res.successful)

    def test_can_list_compare_exchange(self):
        user1 = User("Karmel")
        res1: CompareExchangeResult[User] = self.store.operations.send(
            PutCompareExchangeValueOperation("test", user1, 0)
        )

        user2 = User("Karmel")
        res2: CompareExchangeResult[User] = self.store.operations.send(
            PutCompareExchangeValueOperation("test2", user2, 0)
        )

        self.assertEqual("Karmel", res1.value.name)
        self.assertTrue(res1.successful)

        self.assertEqual("Karmel", res2.value.name)
        self.assertTrue(res2.successful)

        values: Dict[str, CompareExchangeValue[User]] = self.store.operations.send(
            GetCompareExchangeValuesOperation(StartingWithOptions("test"), User)
        )
        self.assertEqual(2, len(values))

        self.assertEqual("Karmel", values.get("test").value.name)
        self.assertEqual("Karmel", values.get("test2").value.name)

    def test_remove_unique_failed(self):
        res: CompareExchangeResult[str] = self.store.operations.send(
            PutCompareExchangeValueOperation("test", "Karmel", 0)
        )
        self.assertEqual("Karmel", res.value)
        self.assertTrue(res.successful)

        res = self.store.operations.send(DeleteCompareExchangeValueOperation(str, "test", 0))
        self.assertFalse(res.successful)

        read_value: CompareExchangeValue[str] = self.store.operations.send(
            GetCompareExchangeValueOperation("test", str)
        )
        self.assertEqual("Karmel", read_value.value)

    def test_return_current_value_when_putting_concurrently(self):
        user = User("Karmel")
        user2 = User("Karmel2")

        res: CompareExchangeResult[User] = self.store.operations.send(PutCompareExchangeValueOperation("test", user, 0))
        res2: CompareExchangeResult[User] = self.store.operations.send(
            PutCompareExchangeValueOperation("test", user2, 0)
        )

        self.assertTrue(res.successful)
        self.assertFalse(res2.successful)
        self.assertEqual("Karmel", res.value.name)
        self.assertEqual("Karmel", res2.value.name)

        user3 = User("Karmel2")
        res2 = self.store.operations.send(PutCompareExchangeValueOperation("test", user3, res2.index))
        self.assertTrue(res2.successful)
        self.assertEqual("Karmel2", res2.value.name)

    def test_can_add_metadata_to_simple_compare_exchange(self):
        string = "Test"
        num = 123.456
        key = "egr/test/cmp/x/change/simple"

        session_options = SessionOptions(transaction_mode=TransactionMode.CLUSTER_WIDE)

        with self.store.open_session(session_options=session_options) as session:
            result = session.advanced.cluster_transaction.create_compare_exchange_value(key, 322)
            result.metadata["TestString"] = string
            result.metadata["TestNumber"] = num
            session.save_changes()

        res: CompareExchangeValue[int] = self.store.operations.send(GetCompareExchangeValueOperation(key, int))

        self.assertIsNotNone(res.metadata)
        self.assertEqual(322, res.value)
        self.assertEqual(string, res.metadata["TestString"])
        self.assertEqual(num, res.metadata["TestNumber"])

        stats: DetailedDatabaseStatistics = self.store.maintenance.send(GetDetailedStatisticsOperation())
        self.assertEqual(1, stats.count_of_compare_exchange)

    def test_can_add_metadata_to_simple_compare_exchange_array(self):
        string = "Test"
        key = "egr/test/cmp/x/change/simple"

        session_options = SessionOptions(transaction_mode=TransactionMode.CLUSTER_WIDE)

        with self.store.open_session(session_options=session_options) as session:
            result = session.advanced.cluster_transaction.create_compare_exchange_value(key, ["a", "b", "c"])
            result.metadata["TestString"] = string
            session.save_changes()

        res = self.store.operations.send(GetCompareExchangeValueOperation(key, list))
        self.assertIsNotNone(res.metadata)

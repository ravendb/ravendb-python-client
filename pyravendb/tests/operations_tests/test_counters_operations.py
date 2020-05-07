from pyravendb.tests.test_base import *
from pyravendb.raven_operations.counters_operations import *
import unittest


class TestCountersOperations(TestBase):
    def setUp(self):
        super(TestCountersOperations, self).setUp()
        with self.store.open_session() as session:
            session.store(User(name="Idan", age="31"), key="users/1-A")
            session.store(User(name="Lee", age="31"), key="users/2-A")
            session.save_changes()

    def tearDown(self):
        super(TestCountersOperations, self).tearDown()
        TestBase.delete_all_topology_files()

    def test_get_one_counter(self):
        with self.store.open_session() as session:
            document_counter = session.counters_for("users/1-A")
            document_counter.increment("Likes", delta=10)
            session.save_changes()

        details = self.store.operations.send(GetCountersOperation(document_id="users/1-A", counters="Likes"))
        self.assertIsNotNone(details.get("Counters", None))
        counters = details["Counters"]
        self.assertEqual(len(counters), 1)
        self.assertEqual(counters[0]["TotalValue"], 10)
        self.assertEqual(counters[0]["CounterName"], "Likes")

    def test_get_multi_counters(self):
        with self.store.open_session() as session:
            document_counter = session.counters_for("users/1-A")
            document_counter.increment("Likes", delta=10)
            document_counter.increment("Shares", delta=120)
            session.save_changes()

        details = self.store.operations.send(
            GetCountersOperation(document_id="users/1-A", counters=["Likes", "Shares"]))
        self.assertIsNotNone(details.get("Counters", None))
        counters = details["Counters"]
        self.assertEqual(len(counters), 2)
        for counter in counters:
            if counter["CounterName"] == "Likes":
                self.assertEqual(counter["TotalValue"], 10)
            else:
                self.assertEqual(counter["TotalValue"], 120)

    def test_increment_counters(self):
        counter_operation = DocumentCountersOperation(document_id='users/1-A')
        counter_operation.add_operations(
            CounterOperation("Likes", counter_operation_type=CounterOperationType.increment, delta=4))
        counter_operation.add_operations(
            CounterOperation("Shares", counter_operation_type=CounterOperationType.increment, delta=4))
        counter_operation.add_operations(
            CounterOperation("Shares", counter_operation_type=CounterOperationType.increment, delta=22))

        counter_batch = CounterBatch([counter_operation])
        results = self.store.operations.send(CounterBatchOperation(counter_batch))
        self.assertIsNotNone(results.get("Counters", None))
        counters = results["Counters"]
        self.assertEqual(len(counters), 3)
        self.assertTrue(any(counter["DocumentId"] == "users/1-A" for counter in counters))
        self.assertTrue(counters[0]["CounterName"] == "Likes" and counters[0]["TotalValue"] == 4)
        self.assertTrue(counters[1]["CounterName"] == "Shares" and counters[1]["TotalValue"] == 4)
        self.assertTrue(counters[2]["CounterName"] == "Shares" and counters[2]["TotalValue"] == 26)

    def test_delete_counters(self):
        counter_operation = DocumentCountersOperation(document_id='users/1-A')
        counter_operation.add_operations(
            CounterOperation("Likes", counter_operation_type=CounterOperationType.increment, delta=4))
        counter_operation.add_operations(
            CounterOperation("Shares", counter_operation_type=CounterOperationType.increment, delta=422))
        counter_operation.add_operations(
            CounterOperation("Likes", counter_operation_type=CounterOperationType.delete))

        counter_batch = CounterBatch([counter_operation])
        results = self.store.operations.send(CounterBatchOperation(counter_batch))
        self.assertIsNotNone(results)
        self.assertEqual(len(results.get("Counters", [])), 2)

        results = self.store.operations.send(GetCountersOperation(document_id="users/1-A"))
        self.assertIsNotNone(results)
        self.assertEqual(len(results.get("Counters", [])), 1)
        counters = results.get("Counters", [])
        self.assertTrue(counters[0].get("CounterName", "") == "Shares")
        print(results)

    def test_send_multi_operations(self):
        counter_operation1 = DocumentCountersOperation(document_id='users/1-A')
        counter_operation1.add_operations(
            CounterOperation("Likes", counter_operation_type=CounterOperationType.increment, delta=4))
        counter_operation1.add_operations(
            CounterOperation("Shares", counter_operation_type=CounterOperationType.increment, delta=422))

        counter_operation2 = DocumentCountersOperation(document_id='users/2-A')
        counter_operation2.add_operations(
            CounterOperation("Likes", counter_operation_type=CounterOperationType.increment, delta=600))
        counter_operation2.add_operations(
            CounterOperation("Shares", counter_operation_type=CounterOperationType.increment, delta=450))

        counter_batch = CounterBatch([counter_operation1, counter_operation2])
        results = self.store.operations.send(CounterBatchOperation(counter_batch))
        counters = results.get("Counters", None)
        self.assertIsNotNone(counters)
        self.assertEqual(len(counters), 4)
        self.assertEqual(counters[0].get("DocumentId", ""), "users/1-A")
        self.assertEqual(counters[2].get("DocumentId", ""), "users/2-A")


if __name__ == '__main__':
    unittest.main()

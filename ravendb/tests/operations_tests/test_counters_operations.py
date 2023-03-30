from ravendb.documents.operations.counters import DocumentCountersOperation, CounterOperation, CounterOperationType, \
    CounterBatch, CounterBatchOperation, GetCountersOperation
from ravendb.tests.test_base import *
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

        details = self.store.operations.send(GetCountersOperation("users/1-A", counters="Likes"))
        self.assertIsNotNone(details.counters)
        counters = details.counters
        self.assertEqual(len(counters), 1)
        self.assertEqual(counters[0].total_value, 10)
        self.assertEqual(counters[0].counter_name, "Likes")

    def test_get_multi_counters(self):
        with self.store.open_session() as session:
            document_counter = session.counters_for("users/1-A")
            document_counter.increment("Likes", delta=10)
            document_counter.increment("Shares", delta=120)
            session.save_changes()

        details = self.store.operations.send(
            GetCountersOperation("users/1-A", counters=["Likes", "Shares"])
        )
        self.assertIsNotNone(details.counters)
        counters = details.counters
        self.assertEqual(len(counters), 2)
        for counter in counters:
            if counter.counter_name == "Likes":
                self.assertEqual(counter.total_value, 10)
            else:
                self.assertEqual(counter.total_value, 120)

    def test_increment_counters(self):
        counter_operation = DocumentCountersOperation(document_id="users/1-A", operations=[])
        counter_operation.add_operations(
            CounterOperation("Likes", counter_operation_type=CounterOperationType.INCREMENT, delta=4)
        )
        counter_operation.add_operations(
            CounterOperation("Shares", counter_operation_type=CounterOperationType.INCREMENT, delta=4)
        )
        counter_operation.add_operations(
            CounterOperation(
                "Shares",
                counter_operation_type=CounterOperationType.INCREMENT,
                delta=22,
            )
        )

        counter_batch = CounterBatch(documents=[counter_operation])
        results = self.store.operations.send(CounterBatchOperation(counter_batch))
        self.assertIsNotNone(results.counters)
        counters = results.counters
        self.assertEqual(len(counters), 3)
        self.assertTrue(any(counter.document_id == "users/1-A" for counter in counters))
        self.assertTrue(counters[0].counter_name == "Likes" and counters[0].total_value == 4)
        self.assertTrue(counters[1].counter_name == "Shares" and counters[1].total_value == 4)
        self.assertTrue(counters[2].counter_name == "Shares" and counters[2].total_value == 26)

    def test_delete_counters(self):
        counter_operation = DocumentCountersOperation(document_id="users/1-A", operations=[])
        counter_operation.add_operations(
            CounterOperation("Likes", counter_operation_type=CounterOperationType.INCREMENT, delta=4)
        )
        counter_operation.add_operations(
            CounterOperation(
                "Shares",
                counter_operation_type=CounterOperationType.INCREMENT,
                delta=422,
            )
        )
        counter_operation.add_operations(CounterOperation("Likes", counter_operation_type=CounterOperationType.DELETE))

        counter_batch = CounterBatch(documents=[counter_operation])
        results = self.store.operations.send(CounterBatchOperation(counter_batch))
        self.assertIsNotNone(results)
        self.assertEqual(len(results.counters), 2)

        results = self.store.operations.send(GetCountersOperation("users/1-A"))
        self.assertIsNotNone(results)
        self.assertEqual(len(results.counters), 1)
        counters = results.counters
        self.assertTrue(counters[0].counter_name == "Shares")

    def test_send_multi_operations(self):
        counter_operation1 = DocumentCountersOperation(document_id="users/1-A", operations=[])
        counter_operation1.add_operations(
            CounterOperation("Likes", counter_operation_type=CounterOperationType.INCREMENT, delta=4)
        )
        counter_operation1.add_operations(
            CounterOperation(
                "Shares",
                counter_operation_type=CounterOperationType.INCREMENT,
                delta=422,
            )
        )

        counter_operation2 = DocumentCountersOperation(document_id="users/2-A", operations=[])
        counter_operation2.add_operations(
            CounterOperation(
                "Likes",
                counter_operation_type=CounterOperationType.INCREMENT,
                delta=600,
            )
        )
        counter_operation2.add_operations(
            CounterOperation(
                "Shares",
                counter_operation_type=CounterOperationType.INCREMENT,
                delta=450,
            )
        )

        counter_batch = CounterBatch(documents=[counter_operation1, counter_operation2])
        results = self.store.operations.send(CounterBatchOperation(counter_batch))
        counters = results.counters
        self.assertIsNotNone(counters)
        self.assertEqual(len(counters), 4)
        self.assertEqual(counters[0].document_id, "users/1-A")
        self.assertEqual(counters[2].document_id, "users/2-A")


if __name__ == "__main__":
    unittest.main()

from ravendb.documents.operations.counters import (
    DocumentCountersOperation,
    CounterOperation,
    CounterOperationType,
    CounterBatch,
    CounterBatchOperation, GetCountersOperation,
)
from ravendb.infrastructure.entities import User
from ravendb.tests.test_base import TestBase


class TestCountersSingleNode(TestBase):
    def setUp(self):
        super(TestCountersSingleNode, self).setUp()

    def test_increment_counter(self):
        with self.store.open_session() as session:
            user = User(name="Aviv")
            session.store(user, "users/1-A")
            session.save_changes()

        document_counters_operation = DocumentCountersOperation(
            "users/1-A", [CounterOperation.create("likes", CounterOperationType.INCREMENT, 0)]
        )

        counter_batch = CounterBatch()
        counter_batch.documents = [document_counters_operation]

        self.store.operations.send(CounterBatchOperation(counter_batch))

        details = self.store.operations.send(GetCountersOperation("users/1-A", ["likes"]))
        val = details.counters[0].total_value
        self.assertEqual(0, val)

        document_counters_operation = DocumentCountersOperation("users/1-A", [CounterOperation.create("likes",CounterOperationType.INCREMENT,10)])

        counter_batch = CounterBatch(documents=[document_counters_operation])

        self.store.operations.send(CounterBatchOperation(counter_batch))

        details = self.store.operations.send(GetCountersOperation("users/1-A", ["likes"]))
        val = details.counters[0].total_value

        self.assertEqual(10, val)

        document_counters_operation = DocumentCountersOperation("users/1-A", [CounterOperation.create("likes", CounterOperationType.INCREMENT, -3)])

        counter_batch = CounterBatch(documents=[document_counters_operation])

        self.store.operations.send(CounterBatchOperation(counter_batch))

        details = self.store.operations.send(GetCountersOperation("users/1-A", ["likes"]))
        val = details.counters[0].total_value

        self.assertEqual(7, val)





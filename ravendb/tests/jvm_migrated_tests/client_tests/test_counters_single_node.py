from ravendb import constants
from ravendb.documents.operations.counters import (
    DocumentCountersOperation,
    CounterOperation,
    CounterOperationType,
    CounterBatch,
    CounterBatchOperation,
    GetCountersOperation,
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

        document_counters_operation = DocumentCountersOperation(
            "users/1-A", [CounterOperation.create("likes", CounterOperationType.INCREMENT, 10)]
        )

        counter_batch = CounterBatch(documents=[document_counters_operation])

        self.store.operations.send(CounterBatchOperation(counter_batch))

        details = self.store.operations.send(GetCountersOperation("users/1-A", ["likes"]))
        val = details.counters[0].total_value

        self.assertEqual(10, val)

        document_counters_operation = DocumentCountersOperation(
            "users/1-A", [CounterOperation.create("likes", CounterOperationType.INCREMENT, -3)]
        )

        counter_batch = CounterBatch(documents=[document_counters_operation])

        self.store.operations.send(CounterBatchOperation(counter_batch))

        details = self.store.operations.send(GetCountersOperation("users/1-A", ["likes"]))
        val = details.counters[0].total_value

        self.assertEqual(7, val)

    def test_delete_create_with_same_name_delete_again(self):
        with self.store.open_session() as session:
            user = User(name="Aviv")
            session.store(user, "users/1-A")
            session.save_changes()

        document_counters_operation_1 = DocumentCountersOperation(
            "users/1-A", [CounterOperation.create("likes", CounterOperationType.INCREMENT, 10)]
        )

        batch = CounterBatch()
        batch.documents = [document_counters_operation_1]
        self.store.operations.send(CounterBatchOperation(batch))

        self.assertEqual(
            10, self.store.operations.send(GetCountersOperation("users/1-A", ["likes"])).counters[0].total_value
        )

        document_counters_operation_1 = DocumentCountersOperation(
            "users/1-A", [CounterOperation.create("likes", CounterOperationType.DELETE)]
        )

        batch = CounterBatch()
        batch.documents = [document_counters_operation_1]
        self.store.operations.send(CounterBatchOperation(batch))

        counters_detail = self.store.operations.send(GetCountersOperation("users/1-A", ["likes"]))
        self.assertEqual(1, len(counters_detail.counters))
        self.assertIsNone(counters_detail.counters[0])

        document_counters_operation_1 = DocumentCountersOperation(
            "users/1-A", [CounterOperation.create("likes", CounterOperationType.INCREMENT, 20)]
        )

        batch = CounterBatch(documents=[document_counters_operation_1])
        self.store.operations.send(CounterBatchOperation(batch))

        self.assertEqual(
            20, self.store.operations.send(GetCountersOperation("users/1-A", ["likes"])).counters[0].total_value
        )

        document_counters_operation_1 = DocumentCountersOperation(
            "users/1-A", [CounterOperation.create("likes", CounterOperationType.DELETE)]
        )

        batch = CounterBatch(documents=[document_counters_operation_1])
        self.store.operations.send(CounterBatchOperation(batch))

        counters_detail = self.store.operations.send(GetCountersOperation("users/1-A", ["likes"]))
        self.assertEqual(1, len(counters_detail.counters))
        self.assertIsNone(counters_detail.counters[0])

    def test_get_counter_value(self):
        with self.store.open_session() as session:
            user = User(name="Aviv")
            session.store(user)
            session.save_changes()

        document_counters_operation = DocumentCountersOperation(
            "users/1-A", [CounterOperation.create("likes", CounterOperationType.INCREMENT, 5)]
        )

        counter_batch = CounterBatch(documents=[document_counters_operation])

        a = self.store.operations.send(CounterBatchOperation(counter_batch))

        document_counters_operation = DocumentCountersOperation(
            "users/1-A", operations=[CounterOperation.create("likes", CounterOperationType.INCREMENT, 10)]
        )

        counter_batch = CounterBatch(documents=[document_counters_operation])

        b = self.store.operations.send(CounterBatchOperation(counter_batch))

        details = self.store.operations.send(GetCountersOperation("users/1-A", ["likes"]))
        val = details.counters[0].total_value

        self.assertEqual(15, val)

    def test_multi_get(self):
        with self.store.open_session() as session:
            user = User(name="Aviv")
            session.store(user, "users/1-A")
            session.save_changes()

        document_counters_operation1 = DocumentCountersOperation(
            "users/1-A",
            [
                CounterOperation.create("likes", CounterOperationType.INCREMENT, 5),
                CounterOperation.create("dislikes", CounterOperationType.INCREMENT, 10),
            ],
        )

        counter_batch = CounterBatch(documents=[document_counters_operation1])
        self.store.operations.send(CounterBatchOperation(counter_batch))

        counters = self.store.operations.send(GetCountersOperation("users/1-A", ["likes", "dislikes"])).counters
        self.assertEqual(2, len(counters))

        self.assertEqual(5, list(filter(lambda x: x.counter_name == "likes", counters))[0].total_value)
        self.assertEqual(10, list(filter(lambda x: x.counter_name == "dislikes", counters))[0].total_value)

    def test_delete_counter(self):
        with self.store.open_session() as session:
            user1 = User(name="Aviv1")
            user2 = User(name="Aviv2")

            session.store(user1)
            session.store(user2)

            session.save_changes()

        document_counters_operation_1 = DocumentCountersOperation(
            "users/1-A", [CounterOperation.create("likes", CounterOperationType.INCREMENT, 10)]
        )
        document_counters_operation_2 = DocumentCountersOperation(
            "users/2-A", [CounterOperation.create("likes", CounterOperationType.INCREMENT, 20)]
        )

        counter_batch = CounterBatch(documents=[document_counters_operation_1, document_counters_operation_2])
        self.store.operations.send(CounterBatchOperation(counter_batch))

        delete_counter = DocumentCountersOperation(
            "users/1-A", [CounterOperation.create("likes", CounterOperationType.DELETE)]
        )

        counter_batch = CounterBatch(documents=[delete_counter])

        self.store.operations.send(CounterBatchOperation(counter_batch))

        counters_detail = self.store.operations.send(GetCountersOperation("users/1-A", ["likes"]))
        self.assertEqual(1, len(counters_detail.counters))
        self.assertIsNone(counters_detail.counters[0])

        delete_counter = DocumentCountersOperation(
            "users/2-A", [CounterOperation.create("likes", CounterOperationType.DELETE)]
        )

        counter_batch = CounterBatch(documents=[delete_counter])

        self.store.operations.send(CounterBatchOperation(counter_batch))

        counters_detail = self.store.operations.send(GetCountersOperation("users/2-A", ["likes"]))
        self.assertEqual(1, len(counters_detail.counters))
        self.assertIsNone(counters_detail.counters[0])

    def test_multi_set_and_get_via_batch(self):
        with self.store.open_session() as session:
            user1 = User(name="Aviv")
            user2 = User(name="Aviv2")

            session.store(user1, "users/1-A")
            session.store(user2, "users/2-A")
            session.save_changes()

        document_counters_operation_1 = DocumentCountersOperation(
            "users/1-A",
            [
                CounterOperation.create("likes", CounterOperationType.INCREMENT, 5),
                CounterOperation.create("dislikes", CounterOperationType.INCREMENT, 10),
            ],
        )
        document_counters_operation_2 = DocumentCountersOperation(
            "users/2-A", [CounterOperation.create("rank", CounterOperationType.INCREMENT, 20)]
        )

        set_batch = CounterBatch(documents=[document_counters_operation_1, document_counters_operation_2])

        self.store.operations.send(CounterBatchOperation(set_batch))

        document_counters_operation_1 = DocumentCountersOperation(
            "users/1-A",
            [
                CounterOperation.create("likes", CounterOperationType.GET),
                CounterOperation.create("dislikes", CounterOperationType.GET),
            ],
        )
        document_counters_operation_2 = DocumentCountersOperation(
            "users/2-A", [CounterOperation.create("rank", CounterOperationType.GET)]
        )

        get_batch = CounterBatch(documents=[document_counters_operation_1, document_counters_operation_2])

        counters_detail = self.store.operations.send(CounterBatchOperation(get_batch))

        self.assertEqual(3, len(counters_detail.counters))

        self.assertEqual("likes", counters_detail.counters[0].counter_name)
        self.assertEqual("users/1-A", counters_detail.counters[0].document_id)
        self.assertEqual(5, counters_detail.counters[0].total_value)

        self.assertEqual("dislikes", counters_detail.counters[1].counter_name)
        self.assertEqual("users/1-A", counters_detail.counters[1].document_id)
        self.assertEqual(10, counters_detail.counters[1].total_value)

        self.assertEqual("rank", counters_detail.counters[2].counter_name)
        self.assertEqual("users/2-A", counters_detail.counters[2].document_id)
        self.assertEqual(20, counters_detail.counters[2].total_value)

    def test_increment_and_delete_should_change_document_metadata(self):
        with self.store.open_session() as session:
            user = User(name="Aviv")
            session.store(user, "users/1-A")
            session.save_changes()

        document_counters_operation_1 = DocumentCountersOperation(
            "users/1-A", [CounterOperation.create("likes", CounterOperationType.INCREMENT, 10)]
        )

        batch = CounterBatch(documents=[document_counters_operation_1])
        self.store.operations.send(CounterBatchOperation(batch))

        with self.store.open_session() as session:
            user = session.load("users/1-A", User)
            metadata = session.advanced.get_metadata_for(user)

            counters = metadata.get(constants.Documents.Metadata.COUNTERS)
            self.assertEqual(1, len(counters))
            self.assertIn("likes", counters)

        document_counters_operation_1 = DocumentCountersOperation(
            "users/1-A", [CounterOperation.create("votes", CounterOperationType.INCREMENT, 50)]
        )

        batch = CounterBatch(documents=[document_counters_operation_1])
        self.store.operations.send(CounterBatchOperation(batch))

        with self.store.open_session() as session:
            user = session.load("users/1-A", User)
            metadata = session.advanced.get_metadata_for(user)

            counters = metadata[constants.Documents.Metadata.COUNTERS]
            self.assertEqual(2, len(counters))
            self.assertIn("likes", counters)
            self.assertIn("votes", counters)

        document_counters_operation_1 = DocumentCountersOperation(
            "users/1-A", [CounterOperation.create("likes", CounterOperationType.DELETE)]
        )

        batch = CounterBatch(documents=[document_counters_operation_1])
        self.store.operations.send(CounterBatchOperation(batch))

        with self.store.open_session() as session:
            user = session.load("users/1-A", User)
            metadata = session.advanced.get_metadata_for(user)

            counters = metadata[constants.Documents.Metadata.COUNTERS]
            self.assertEqual(1, len(counters))
            self.assertIn("votes", counters)

        document_counters_operation_1 = DocumentCountersOperation(
            "users/1-A", [CounterOperation.create("votes", CounterOperationType.DELETE)]
        )

        batch = CounterBatch(documents=[document_counters_operation_1])
        self.store.operations.send(CounterBatchOperation(batch))

        with self.store.open_session() as session:
            user = session.load("users/1-A", User)
            metadata = session.advanced.get_metadata_for(user)

            counters = metadata.get(constants.Documents.Metadata.COUNTERS, None)
            self.assertIsNone(counters)

    def test_get_counter_value_using_post(self):
        with self.store.open_session() as session:
            user = User(name="Aviv")
            session.store(user, "users/1-A")
            session.save_changes()

        long_counter_name = "a" * 500
        document_counters_operation = DocumentCountersOperation(
            "users/1-A", [CounterOperation.create(long_counter_name, CounterOperationType.INCREMENT, 5)]
        )

        counter_batch = CounterBatch(documents=[document_counters_operation])

        self.store.operations.send(CounterBatchOperation(counter_batch))

        with self.store.open_session() as session:
            dic = session.counters_for("users/1-A").get_many([long_counter_name, "no_such"])
            self.assertEqual(1, len(dic))
            self.assertIn((long_counter_name, 5), dic.items())

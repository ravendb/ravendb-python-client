"""
Counter increment: omitting delta when incrementing defaults to delta=1.

C# reference: SlowTests.Issues/Issues/RavenDB_22150.cs
  IncrementByDefaultValue
"""

from ravendb.documents.operations.counters import (
    CounterBatch,
    CounterBatchOperation,
    CounterOperation,
    CounterOperationType,
    DocumentCountersOperation,
)
from ravendb.infrastructure.entities import User
from ravendb.tests.test_base import TestBase


class TestRavenDB22150(TestBase):
    def setUp(self):
        super().setUp()

    def test_increment_without_delta_defaults_to_one(self):
        """
        CounterOperation(INCREMENT) with no delta argument defaults to delta=1
        on the client, matching the C# client's permissive default.

        C# spec: IncrementByDefaultValue — sends a batch with two operations:
          one with an explicit delta and one with no delta. The no-delta
          operation should increment by 1.
        """
        with self.store.open_session() as session:
            session.store(User(name="Danielle"), "users/1")
            session.save_changes()

        with self.store.open_session() as session:
            session.counters_for("users/1").increment("likes", 10)
            session.counters_for("users/1").increment("dislikes", 20)
            session.save_changes()

        # Single batch: explicit delta=5 for likes, no delta for dislikes (defaults to 1).
        result = self.store.operations.send(
            CounterBatchOperation(
                CounterBatch(
                    documents=[
                        DocumentCountersOperation(
                            document_id="users/1",
                            operations=[
                                CounterOperation("likes", CounterOperationType.INCREMENT, delta=5),
                                CounterOperation("dislikes", CounterOperationType.INCREMENT),
                            ],
                        )
                    ]
                )
            )
        )

        self.assertEqual(21, result.counters[1].total_value)

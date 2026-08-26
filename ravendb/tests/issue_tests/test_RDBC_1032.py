"""
RDBC-1032: Operation.wait_for_completion() returns BulkOperationResult instead of None.

C# reference: FastTests/Client/Operations/BasicChangesOperationsTests.cs
              Can_Perform_Patch_By_Query_Operation()
"""

import unittest
from datetime import timedelta
from unittest.mock import MagicMock, patch

from ravendb.documents.operations.operation import BulkOperationResult, Operation
from ravendb.exceptions.exceptions import InvalidOperationException
from ravendb.tests.test_base import TestBase


class TestBulkOperationResultUnit(unittest.TestCase):
    """Unit tests — no server required."""

    def test_bulk_operation_result_from_json(self):
        result = BulkOperationResult.from_json(
            {"Total": 5, "DocumentsProcessed": 3, "AttachmentsProcessed": 1, "CountersProcessed": 1}
        )
        self.assertEqual(5, result.total)
        self.assertEqual(3, result.documents_processed)
        self.assertEqual(1, result.attachments_processed)
        self.assertEqual(1, result.counters_processed)

    def test_bulk_operation_result_defaults(self):
        result = BulkOperationResult()
        self.assertEqual(0, result.total)
        self.assertEqual(0, result.documents_processed)
        self.assertEqual(0, result.attachments_processed)
        self.assertEqual(0, result.counters_processed)
        self.assertEqual(0, result.time_series_processed)
        self.assertIsNone(result.query)
        self.assertEqual([], result.details)

    def test_bulk_operation_result_message(self):
        result = BulkOperationResult(total=1234)
        self.assertEqual("Processed 1,234 items.", result.message)

    def test_bulk_operation_result_details(self):
        detail = {"Id": "docs/1", "ChangeVector": "A:1", "Status": "Patched"}
        result = BulkOperationResult.from_json({"Total": 1, "Details": [detail]})
        self.assertEqual(1, len(result.details))
        self.assertEqual("docs/1", result.details[0]["Id"])

    def test_bulk_operation_result_from_json_none(self):
        result = BulkOperationResult.from_json(None)
        self.assertEqual(0, result.total)

    def test_bulk_operation_result_from_json_zero_total(self):
        result = BulkOperationResult.from_json({"Total": 0})
        self.assertEqual(0, result.total)
        self.assertEqual(0, result.documents_processed)

    def test_wait_for_completion_raises_on_timeout(self):
        op = Operation(None, None, None, key=1)
        with self.assertRaises(TimeoutError):
            op.wait_for_completion(timeout=timedelta(microseconds=-1))

    def test_fetch_operations_status_raises_when_state_always_null(self):
        # Mocks are unavoidable here: the null-state scenario is a server-side race condition
        # (operation submitted but not yet registered) that cannot be triggered deterministically
        # without a real server. _get_operation_state_command is stubbed to always return
        # result=None; time.sleep is suppressed so the 10-retry loop completes instantly.
        executor = MagicMock()
        op = Operation(executor, None, None, key=42)
        command_stub = MagicMock()
        command_stub.result = None
        with (
            patch.object(op, "_get_operation_state_command", return_value=command_stub),
            patch("ravendb.documents.operations.operation.time.sleep"),
        ):
            with self.assertRaises(InvalidOperationException):
                op.fetch_operations_status()

    def test_bulk_operation_result_from_json_all_fields(self):
        result = BulkOperationResult.from_json(
            {
                "Total": 10,
                "DocumentsProcessed": 4,
                "AttachmentsProcessed": 2,
                "CountersProcessed": 2,
                "TimeSeriesProcessed": 2,
                "Query": "FROM Orders",
            }
        )
        self.assertEqual(10, result.total)
        self.assertEqual(4, result.documents_processed)
        self.assertEqual(2, result.attachments_processed)
        self.assertEqual(2, result.counters_processed)
        self.assertEqual(2, result.time_series_processed)
        self.assertEqual("FROM Orders", result.query)


class TestOperationResult(TestBase):
    """Integration tests — require a live server."""

    def setUp(self):
        super().setUp()
        self.store = self.get_document_store()

    def tearDown(self):
        super().tearDown()
        self.store.close()

    def test_patch_by_query_returns_bulk_result(self):
        from ravendb.documents.operations.patch import PatchByQueryOperation
        from ravendb.infrastructure.orders import Product

        with self.store.open_session() as session:
            p1 = Product()
            p1.name = "Apple"
            p1.price_per_unit = 1.0
            session.store(p1, "products/1")
            p2 = Product()
            p2.name = "Banana"
            p2.price_per_unit = 2.0
            session.store(p2, "products/2")
            session.save_changes()

        patch_op = PatchByQueryOperation("FROM Products UPDATE { this.price_per_unit = this.price_per_unit * 2; }")
        op = self.store.operations.send_async(patch_op)
        result = op.wait_for_completion()

        self.assertIsInstance(result, BulkOperationResult)
        self.assertEqual(2, result.total)
        self.assertEqual([], result.details)

        with self.store.open_session() as session:
            p1 = session.load("products/1", Product)
            p2 = session.load("products/2", Product)
            self.assertAlmostEqual(2.0, p1.price_per_unit)
            self.assertAlmostEqual(4.0, p2.price_per_unit)


if __name__ == "__main__":
    unittest.main()

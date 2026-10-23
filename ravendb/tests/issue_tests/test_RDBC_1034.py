"""
RDBC-1034: store.execute_indexes([]) should be a no-op, not raise ValueError.

C# reference: FastTests/Client/Indexing/IndexesFromClient.cs
              CreateIndexes_Should_Not_Throw_When_Indexes_List_Is_Empty (RavenDB-24077)
"""

import logging
import unittest

from ravendb.documents.operations.indexes import (
    EnableIndexOperation,
    GetIndexesOperation,
    PutIndexesOperation,
)
from ravendb.http.server_node import ServerNode
from ravendb.http.topology import RaftCommand
from ravendb.tests.test_base import TestBase


class TestExecuteIndexesEmptyUnit(unittest.TestCase):
    """Unit tests — no server required."""

    def test_put_indexes_operation_empty_args_raises(self):
        # PutIndexesOperation with no args should raise ValueError (matches C# ArgumentNullException)
        with self.assertRaises(ValueError):
            PutIndexesOperation()

    def test_put_indexes_set_response_parses_error_from_dict(self):
        # set_response must parse the JSON payload before checking for an "Error" field.
        # A raw substring search can misclassify successful payloads and break on dict-style access.
        from ravendb.documents.operations.indexes import PutIndexesOperation
        from ravendb.documents.indexes.definitions import IndexDefinition
        import json

        idx = IndexDefinition()
        idx.name = "TestIndex"
        idx.maps = {"from d in docs select new { d.Name }"}
        op = PutIndexesOperation(idx)

        class FakeConventions:
            pass

        cmd = op.get_command(FakeConventions())

        # A valid-looking success response must not raise
        success_payload = json.dumps([{"Index": "TestIndex", "RaftCommandIndex": 1}])
        cmd.set_response(success_payload, False)
        self.assertEqual(cmd.result[0]["Index"], "TestIndex")

        # A response that contains "Error" only as a substring in index names must not raise
        harmless_payload = json.dumps([{"Index": "ErrorTracker", "RaftCommandIndex": 2}])
        cmd.set_response(harmless_payload, False)
        self.assertEqual(cmd.result[0]["Index"], "ErrorTracker")

    def test_get_indexes_operation_url_includes_page_size(self):
        # The URL must include &pageSize=N, not the malformed &pageSizeN
        node = ServerNode("http://localhost:8080", "TestDb")

        op = GetIndexesOperation(0, 25)

        class FakeConventions:
            pass

        cmd = op.get_command(FakeConventions())
        req = cmd.create_request(node)
        self.assertIn("pageSize=25", req.url)
        self.assertNotIn("pageSize25", req.url)
        self.assertFalse(req.url.endswith(" "), "URL must not have trailing space")

    def test_enable_index_command_is_raft_command(self):
        # C# EnableIndexCommand implements IRaftCommand; Python must match.
        op = EnableIndexOperation("MyIndex")

        class FakeConventions:
            pass

        cmd = op.get_command(FakeConventions())
        self.assertIsInstance(cmd, RaftCommand)
        self.assertTrue(
            callable(getattr(cmd, "get_raft_unique_request_id", None)),
            "EnableIndexCommand must implement get_raft_unique_request_id",
        )
        # Two calls must return different IDs (each request gets its own Raft slot)
        id1 = cmd.get_raft_unique_request_id()
        id2 = cmd.get_raft_unique_request_id()
        self.assertNotEqual(id1, id2)

    def test_create_indexes_empty_list_does_not_log(self):
        from ravendb.documents.indexes.index_creation import IndexCreation

        class FakeStore:
            class FakeMaintenance:
                def send(self, op):
                    raise AssertionError("send() should not be called for empty list")

            maintenance = FakeMaintenance()
            conventions = None

        with self.assertLogs(level=logging.WARNING) as cm:
            logging.warning("sentinel")  # ensure assertLogs doesn't fail on empty
            IndexCreation.create_indexes([], FakeStore())

        self.assertFalse(
            any("Could not create indexes" in line for line in cm.output),
            "Empty-list call must not emit a 'Could not create indexes' log entry",
        )


class TestExecuteIndexesEmpty(TestBase):
    """Integration tests — require a live server."""

    def setUp(self):
        super().setUp()
        self.store = self.get_document_store()

    def tearDown(self):
        super().tearDown()
        self.store.close()

    def test_execute_indexes_empty_list_is_noop(self):
        # Should NOT raise
        self.store.execute_indexes([])

    def test_index_creation_create_indexes_empty_is_noop(self):
        from ravendb.documents.indexes.index_creation import IndexCreation

        # IndexCreation.create_indexes with empty list should not raise
        IndexCreation.create_indexes([], self.store)


if __name__ == "__main__":
    unittest.main()

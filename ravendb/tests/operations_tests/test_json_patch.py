"""
Tests for the RFC 6902 patch document, the standalone JsonPatchOperation, and the batch
command that carries the same operations inside save_changes.
"""

import json
import unittest

from ravendb.documents.commands.batches import CommandType, JsonPatchCommandData
from ravendb.documents.operations.json_patch import (
    JsonPatchDocument,
    JsonPatchOperation,
    JsonPatchResult,
    escape_json_pointer_segment,
)
from ravendb.documents.operations.patch import PatchStatus
from ravendb.http.server_node import ServerNode
from ravendb.tests.test_base import TestBase

DOCUMENT = {
    "Name": "original",
    "Tags": ["a", "b"],
    "@metadata": {"@collection": "Tests"},
}


class TestJsonPatchDocument(unittest.TestCase):
    def test_operations_keep_the_order_they_were_added_in(self):
        patch = JsonPatchDocument().add("/a", 1).remove("/b").replace("/c", 2)

        self.assertEqual(["add", "remove", "replace"], [op["op"] for op in patch.to_json()])

    def test_each_operation_carries_its_path_and_value(self):
        self.assertEqual(
            {"op": "add", "path": "/Name", "value": "x"}, JsonPatchDocument().add("/Name", "x").to_json()[0]
        )
        self.assertEqual({"op": "remove", "path": "/Name"}, JsonPatchDocument().remove("/Name").to_json()[0])

    def test_move_and_copy_carry_a_from(self):
        self.assertEqual({"op": "move", "from": "/a", "path": "/b"}, JsonPatchDocument().move("/a", "/b").to_json()[0])
        self.assertEqual({"op": "copy", "from": "/a", "path": "/b"}, JsonPatchDocument().copy("/a", "/b").to_json()[0])

    def test_builders_chain(self):
        self.assertEqual(3, len(JsonPatchDocument().add("/a", 1).test("/a", 1).remove("/a")))

    def test_it_round_trips(self):
        patch = JsonPatchDocument().add("/a", 1).remove("/b")

        self.assertEqual(patch.to_json(), JsonPatchDocument.from_json(patch.to_json()).to_json())

    def test_pointer_segments_are_escaped(self):
        # RFC 6901: '~' becomes '~0' and '/' becomes '~1', in that order.
        self.assertEqual("~0", escape_json_pointer_segment("~"))
        self.assertEqual("~1", escape_json_pointer_segment("/"))
        self.assertEqual("a~1b~0c", escape_json_pointer_segment("a/b~c"))


class TestJsonPatchCommandData(unittest.TestCase):
    def test_it_serializes_as_a_json_patch_command(self):
        command = JsonPatchCommandData("docs/1", JsonPatchDocument().add("/Name", "x"))
        serialized = command.serialize(None)

        self.assertEqual("docs/1", serialized["Id"])
        self.assertEqual("JsonPatch", serialized["Type"])
        self.assertIsNone(serialized["ChangeVector"])
        self.assertEqual([{"op": "add", "path": "/Name", "value": "x"}], serialized["JsonPatch"]["Operations"])
        self.assertFalse(serialized["ReturnDocument"])

    def test_its_command_type(self):
        self.assertEqual(CommandType.JSON_PATCH, JsonPatchCommandData("docs/1", JsonPatchDocument()).command_type)

    def test_the_server_type_string_parses_back(self):
        self.assertEqual(CommandType.JSON_PATCH, CommandType.from_csharp_value_str("JsonPatch"))

    def test_missing_arguments_are_rejected(self):
        with self.assertRaises(ValueError):
            JsonPatchCommandData("", JsonPatchDocument())
        with self.assertRaises(ValueError):
            JsonPatchCommandData("docs/1", None)


class TestJsonPatchOperationRequest(unittest.TestCase):
    def test_it_patches_the_json_patch_endpoint(self):
        operation = JsonPatchOperation("docs/1", JsonPatchDocument().add("/Name", "x"))
        request = operation.get_command(None, None, None).create_request(ServerNode("http://localhost:8080", "db"))

        self.assertEqual("PATCH", request.method)
        self.assertEqual("http://localhost:8080/databases/db/json-patch?id=docs%2F1", request.url)
        self.assertEqual({"Operations": [{"op": "add", "path": "/Name", "value": "x"}]}, request.data)

    def test_it_is_not_a_read_request(self):
        self.assertFalse(
            JsonPatchOperation("docs/1", JsonPatchDocument()).get_command(None, None, None).is_read_request()
        )

    def test_it_reads_the_status_back(self):
        command = JsonPatchOperation("docs/1", JsonPatchDocument()).get_command(None, None, None)
        command.set_response(json.dumps({"Status": "Patched", "ModifiedDocument": {"Name": "x"}}), False)

        self.assertIsInstance(command.result, JsonPatchResult)
        self.assertEqual(PatchStatus.PATCHED, command.result.status)
        self.assertEqual({"Name": "x"}, command.result.modified_document)

    def test_missing_arguments_are_rejected(self):
        with self.assertRaises(ValueError):
            JsonPatchOperation("", JsonPatchDocument())
        with self.assertRaises(ValueError):
            JsonPatchOperation("docs/1", None)


class TestJsonPatchOperationAgainstServer(TestBase):
    def test_it_applies_operations_in_order(self):
        with self.store.open_session() as session:
            session.store(dict(DOCUMENT), "docs/1")
            session.save_changes()

        patch = JsonPatchDocument().add("/Name", "patched").add("/Tags/-", "c").replace("/Tags/0", "z")
        result = self.store.operations.send(JsonPatchOperation("docs/1", patch))

        self.assertEqual(PatchStatus.PATCHED, result.status)

        with self.store.open_session() as session:
            document = session.load("docs/1", dict)
            self.assertEqual("patched", document["Name"])
            self.assertEqual(["z", "b", "c"], document["Tags"])

    def test_patching_a_document_that_is_not_there(self):
        self.assertRaisesWithMessageContaining(
            self.store.operations.send,
            Exception,
            "Cannot apply json patch",
            JsonPatchOperation("docs/nope", JsonPatchDocument().add("/Name", "x")),
        )

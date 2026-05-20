# Wire-shape tests for AI conversation attachments. Positional layout matches
# AbstractAiAgentProcessor.ParseMultipartAsync: body, commands, streams.
import io
import json
import unittest

from ravendb.documents.commands.batches import CopyAttachmentCommandData, PutAttachmentCommandData
from ravendb.documents.operations.ai.agents import (
    AiAgentActionRequest,
    AiAgentActionRequestType,
    AiConversationCreationOptions,
    AiConversationParameter,
    AiConversationParameterOptions,
)
from ravendb.documents.operations.ai.agents.run_conversation_operation import RunConversationCommand
from ravendb.http.server_node import ServerNode


def _node() -> ServerNode:
    return ServerNode(url="http://localhost:8080", database="db1", cluster_tag="A")


class TestRunConversationCommandNoAttachments(unittest.TestCase):
    def test_request_is_plain_json(self):
        cmd = RunConversationCommand("agent-1", "chats/1")
        req = cmd.create_request(_node())
        self.assertIsNotNone(req.data)
        self.assertIn("ActionResponses", req.data)
        self.assertFalse(req.files)


class TestRunConversationCommandMultipart(unittest.TestCase):
    def test_files_are_ordered_body_commands_streams(self):
        stream_a = io.BytesIO(b"hello")
        stream_b = io.BytesIO(b"world")
        put_a = PutAttachmentCommandData("__this__", "a.txt", stream_a, "text/plain", change_vector=None)
        put_b = PutAttachmentCommandData("__this__", "b.txt", stream_b, "text/plain", change_vector=None)
        copy = CopyAttachmentCommandData("docs/orig", "pic.png", "__this__", "pic.png", change_vector=None)

        cmd = RunConversationCommand("agent-1", "chats/1", attachments_commands=[put_a, put_b, copy])
        req = cmd.create_request(_node())

        keys = list(req.files.keys())
        # The server's MultipartReader is positional — order matters.
        self.assertEqual(["body", "commands", "a.txt", "b.txt"], keys)

    def test_body_section_contains_attachment_commands(self):
        sa = io.BytesIO(b"x")
        ca = PutAttachmentCommandData("__this__", "a.txt", sa, "text/plain", change_vector=None)
        cmd = RunConversationCommand("agent-1", "chats/1", attachments_commands=[ca])
        req = cmd.create_request(_node())
        body = json.loads(req.files["body"][1])
        self.assertIn("AttachmentCommands", body)
        self.assertEqual(1, len(body["AttachmentCommands"]))
        self.assertEqual("AttachmentPUT", body["AttachmentCommands"][0]["Type"])

    def test_commands_section_mirrors_bulk_docs_shape(self):
        sa = io.BytesIO(b"x")
        ca = PutAttachmentCommandData("__this__", "a.txt", sa, "text/plain", change_vector=None)
        copy = CopyAttachmentCommandData("docs/orig", "pic.png", "__this__", "pic.png", change_vector=None)
        cmd = RunConversationCommand("agent-1", "chats/1", attachments_commands=[ca, copy])
        req = cmd.create_request(_node())
        commands_body = json.loads(req.files["commands"][1])
        # Must match the shape that the v7.2 bulk_docs MultipartReader parses.
        self.assertEqual(["Commands"], list(commands_body.keys()))
        self.assertEqual(2, len(commands_body["Commands"]))
        self.assertEqual("AttachmentPUT", commands_body["Commands"][0]["Type"])
        self.assertEqual("AttachmentCOPY", commands_body["Commands"][1]["Type"])

    def test_stream_parts_carry_command_type_header(self):
        sa = io.BytesIO(b"x")
        ca = PutAttachmentCommandData("__this__", "a.txt", sa, "text/plain", change_vector=None)
        cmd = RunConversationCommand("agent-1", "chats/1", attachments_commands=[ca])
        req = cmd.create_request(_node())
        stream_entry = req.files["a.txt"]
        # (filename, stream, content_type, headers)
        self.assertEqual("a.txt", stream_entry[0])
        self.assertIs(sa, stream_entry[1])
        self.assertEqual("text/plain", stream_entry[2])
        self.assertEqual({"Command-Type": "AttachmentStream"}, stream_entry[3])

    def test_duplicate_stream_rejected(self):
        shared = io.BytesIO(b"x")
        a = PutAttachmentCommandData("__this__", "a.txt", shared, "text/plain", change_vector=None)
        b = PutAttachmentCommandData("__this__", "b.txt", shared, "text/plain", change_vector=None)
        with self.assertRaises(RuntimeError) as cm:
            RunConversationCommand("agent-1", "chats/1", attachments_commands=[a, b])
        self.assertIn("re-use the same stream", str(cm.exception))

    def test_raft_id_set_when_conversation_ends_with_pipe(self):
        # In the v7.2.3 refactor the raft id moves to the command ctor.
        cmd_pipe = RunConversationCommand("agent-1", "chats/1|")
        cmd_no_pipe = RunConversationCommand("agent-1", "chats/1")
        self.assertNotEqual("", cmd_pipe._raft_id)
        # The "don't care" id is a fixed sentinel, not a fresh GUID.
        self.assertNotEqual(cmd_no_pipe._raft_id, cmd_pipe._raft_id)


class TestAiConversationCreationOptionsParametersShim(unittest.TestCase):
    def test_legacy_raw_value(self):
        opts = AiConversationCreationOptions()
        opts.add_parameter("plain", "value-A")
        out = opts.to_json()
        self.assertEqual({"Value": "value-A", "SendToModel": True}, out["Parameters"]["plain"])

    def test_legacy_constructor_dict(self):
        opts = AiConversationCreationOptions(parameters={"a": 1, "b": "two"})
        out = opts.to_json()
        self.assertEqual({"Value": 1, "SendToModel": True}, out["Parameters"]["a"])
        self.assertEqual({"Value": "two", "SendToModel": True}, out["Parameters"]["b"])

    def test_options_controls_send_to_model(self):
        opts = AiConversationCreationOptions()
        opts.add_parameter("hidden", "secret", AiConversationParameterOptions(send_to_model=False))
        out = opts.to_json()
        self.assertEqual({"Value": "secret", "SendToModel": False}, out["Parameters"]["hidden"])

    def test_explicit_parameter_instance(self):
        opts = AiConversationCreationOptions()
        opts.add_parameter("explicit", AiConversationParameter(value=42, send_to_model=False))
        out = opts.to_json()
        self.assertEqual({"Value": 42, "SendToModel": False}, out["Parameters"]["explicit"])

    def test_max_model_iterations_emitted(self):
        opts = AiConversationCreationOptions(max_model_iterations_per_call=7)
        self.assertEqual(7, opts.to_json()["MaxModelIterationsPerCall"])


class TestAiAgentActionRequestHelpers(unittest.TestCase):
    def _make(self, **overrides):
        defaults = dict(
            name="X",
            tool_id="t1",
            arguments="{}",
            type=AiAgentActionRequestType.SUB_AGENT,
            sub_conversation_id="sub-1",
        )
        defaults.update(overrides)
        return AiAgentActionRequest(**defaults)

    def test_eq_same_fields(self):
        self.assertEqual(self._make(), self._make())

    def test_neq_differs_in_name(self):
        self.assertNotEqual(self._make(), self._make(name="Y"))

    def test_neq_differs_in_type(self):
        self.assertNotEqual(self._make(), self._make(type=AiAgentActionRequestType.USER_ACTION))

    def test_hashable_in_set(self):
        s = {self._make(), self._make(name="Y"), self._make()}
        self.assertEqual(2, len(s))

    def test_repr_is_json(self):
        r = self._make()
        # Should parse back to the same JSON shape as to_json.
        parsed = json.loads(repr(r))
        self.assertEqual(r.to_json(), parsed)


if __name__ == "__main__":
    unittest.main()

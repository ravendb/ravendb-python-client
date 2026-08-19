"""Tests for the AI conversation-messages surface, cancelPendingActionTools on
the wire, and the streaming read that honors cancellation.
"""

import json
import threading
import unittest
from datetime import datetime

from ravendb.documents.ai.ai_operations import AiOperations
from ravendb.documents.operations.ai.agents import (
    AiConversationDetailLevel,
    AiConversationMessage,
    AiConversationMessagesResult,
    AiMessageRole,
    AiToolCallResult,
    GetConversationMessagesCommand,
    GetConversationMessagesOperation,
    GetConversationMessagesOptions,
    RunConversationOperation,
)
from ravendb.documents.operations.ai.agents.run_conversation_operation import RunConversationCommand
from ravendb.http.server_node import ServerNode
from ravendb.tools.utils import Utils


class TestGetConversationMessagesCommandWire(unittest.TestCase):
    def setUp(self):
        self.node = ServerNode("http://localhost:8080", "db1")

    def _command(self, **kwargs):
        options = GetConversationMessagesOptions(**kwargs)
        operation = GetConversationMessagesOperation(options)
        return operation.get_command(None)

    def test_get_to_conversation_messages_endpoint(self):
        command = self._command(conversation_id="chats/1")
        request = command.create_request(self.node)
        self.assertEqual("GET", request.method)
        self.assertTrue(request.url.startswith("http://localhost:8080/databases/db1/ai/agent/conversation/messages?"))

    def test_parameter_order_and_always_present_params(self):
        before = datetime(2026, 1, 2, 3, 4, 5, 123456)
        command = self._command(
            conversation_id="chats/1", before=before, page_size=50, detail_level=AiConversationDetailLevel.DETAILED
        )
        url = command.create_request(self.node).url
        query = url.split("?", 1)[1]
        parts = [p.split("=", 1)[0] for p in query.split("&")]
        self.assertEqual(["conversationId", "before", "pageSize", "detailLevel"], parts)
        self.assertTrue(url.endswith("&pageSize=50&detailLevel=Detailed"))

    def test_after_appended_only_when_set(self):
        after = datetime(2026, 1, 2, 3, 4, 5, 123456)
        url = self._command(conversation_id="c", after=after).create_request(self.node).url
        self.assertIn("&after=", url)
        self.assertNotIn("&before=", url)

    def test_before_timestamp_is_the_seven_digit_raven_form(self):
        from urllib.parse import unquote

        before = datetime(2026, 1, 2, 3, 4, 5, 123456)
        url = self._command(conversation_id="c", before=before).create_request(self.node).url
        ts = Utils.datetime_to_string(before)
        self.assertEqual(7, len(ts.split(".")[1]))
        query = url.split("?", 1)[1]
        before_value = [p.split("=", 1)[1] for p in query.split("&") if p.startswith("before=")][0]
        self.assertEqual(ts, unquote(before_value))

    def test_page_size_default_is_int_max_value(self):
        url = self._command(conversation_id="c").create_request(self.node).url
        self.assertTrue(url.endswith("&pageSize=2147483647&detailLevel=Simple"))

    def test_is_read_request(self):
        self.assertTrue(self._command(conversation_id="c").is_read_request())

    def test_constructor_validates_conversation_id(self):
        with self.assertRaises(ValueError):
            GetConversationMessagesOperation("")
        with self.assertRaises(ValueError):
            GetConversationMessagesOperation(GetConversationMessagesOptions())

    def test_before_and_after_are_mutually_exclusive(self):
        when = datetime(2026, 1, 1)
        with self.assertRaises(ValueError) as ctx:
            GetConversationMessagesOperation(
                GetConversationMessagesOptions(conversation_id="c", before=when, after=when)
            )
        self.assertIn("cannot both be specified", str(ctx.exception))

    def test_page_size_must_be_positive(self):
        with self.assertRaises(ValueError) as ctx:
            GetConversationMessagesOperation(GetConversationMessagesOptions(conversation_id="c", page_size=0))
        self.assertIn("PageSize must be greater than 0", str(ctx.exception))

    def test_null_response_leaves_result_none(self):
        command = GetConversationMessagesCommand(GetConversationMessagesOptions(conversation_id="missing"))
        command.set_response(None, False)
        self.assertIsNone(command.result)

    def test_parameters_keep_typed_values(self):
        payload = {
            "ConversationId": "chats/1",
            "Agent": "a1",
            "Parameters": {
                "big": 3000000000,
                "frac": 0.5,
                "text": "hi",
                "flag": True,
                "arr": [1, 2, 3],
                "mixed": [1, "x"],
            },
            "TotalUsage": None,
            "Messages": [],
        }
        command = GetConversationMessagesCommand(GetConversationMessagesOptions(conversation_id="chats/1"))
        command.set_response(json.dumps(payload), False)
        params = command.result.parameters
        self.assertIsInstance(params["big"], int)
        self.assertEqual(3000000000, params["big"])
        self.assertIsInstance(params["frac"], float)
        self.assertEqual(0.5, params["frac"])
        self.assertEqual("hi", params["text"])
        self.assertIs(True, params["flag"])
        self.assertEqual([1, 2, 3], params["arr"])
        self.assertEqual([1, "x"], params["mixed"])


class TestAiConversationMessage(unittest.TestCase):
    def test_from_json_parses_all_fields(self):
        payload = {
            "Role": "User",
            "Content": "hello",
            "Attachments": ["a.txt"],
            "Timestamp": "2026-01-02T03:04:05.1234560Z",
            "ToolCalls": [{"Id": "t1", "Name": "n", "Arguments": "{}", "Result": "ok", "SubConversationId": None}],
            "Usage": {
                "PromptTokens": 1,
                "CompletionTokens": 2,
                "TotalTokens": 3,
                "CachedTokens": 0,
                "ReasoningTokens": 0,
            },
            "SubConversationId": None,
        }
        message = AiConversationMessage.from_json(payload)
        self.assertEqual(AiMessageRole.USER, message.role)
        self.assertEqual("hello", message.content)
        self.assertEqual(["a.txt"], message.attachments)
        self.assertEqual(2026, message.timestamp.year)
        self.assertEqual("t1", message.tool_calls[0].id)
        self.assertEqual(AiUsage_tokens(message), 3)

    def test_to_json_order_and_role_as_name(self):
        message = AiConversationMessage(
            role=AiMessageRole.SYSTEM,
            content="sys",
            attachments=[],
            timestamp=datetime(2026, 1, 1),
            tool_calls=[],
            usage=None,
            sub_conversation_id=None,
        )
        result = message.to_json()
        self.assertEqual(
            list(result.keys()),
            ["Role", "Content", "Attachments", "Timestamp", "ToolCalls", "Usage", "SubConversationId"],
        )
        self.assertEqual("System", result["Role"])

    def test_pending_tool_call_result_stays_none(self):
        tool_call = AiToolCallResult.from_json({"Id": "t1", "Name": "n", "Arguments": "{}", "Result": None})
        self.assertIsNone(tool_call.result)
        self.assertEqual(
            {"Id": "t1", "Name": "n", "Arguments": "{}", "Result": None, "SubConversationId": None},
            tool_call.to_json(),
        )

    def test_missing_keys_use_defaults(self):
        message = AiConversationMessage.from_json({"Role": "Assistant"})
        self.assertIsNone(message.content)
        self.assertIsNone(message.attachments)
        self.assertIsNone(message.timestamp)
        self.assertIsNone(message.tool_calls)


def AiUsage_tokens(message):
    return message.usage.total_tokens


class TestAiConversationMessagesResult(unittest.TestCase):
    def test_to_json_key_order(self):
        result = AiConversationMessagesResult(
            conversation_id="c",
            agent="a",
            parameters={},
            total_usage=None,
            last_message_at=None,
            has_more_messages=False,
            sub_conversation_ids=None,
            attachments=None,
            messages=None,
        )
        keys = list(result.to_json().keys())
        self.assertEqual(
            [
                "ConversationId",
                "Agent",
                "Parameters",
                "TotalUsage",
                "LastMessageAt",
                "HasMoreMessages",
                "SubConversationIds",
                "Attachments",
                "Messages",
            ],
            keys,
        )

    def test_missing_key_defaults(self):
        result = AiConversationMessagesResult.from_json({"ConversationId": "c"})
        self.assertEqual("c", result.conversation_id)
        self.assertIsNone(result.agent)
        self.assertIsNone(result.parameters)
        self.assertIsNone(result.total_usage)
        self.assertIsNone(result.last_message_at)
        self.assertIsNone(result.messages)
        self.assertFalse(result.has_more_messages)
        self.assertIsNone(result.sub_conversation_ids)
        self.assertIsNone(result.attachments)


class TestAiOperationsIntegration(unittest.TestCase):
    def test_get_conversation_messages_reachable_from_store(self):
        self.assertTrue(hasattr(AiOperations, "get_conversation_messages"))

    def test_ai_operations_exported_from_package(self):
        import ravendb

        self.assertTrue(hasattr(ravendb, "AiConversationMessagesResult"))
        self.assertTrue(hasattr(ravendb, "GetConversationMessagesOperation"))
        self.assertTrue(hasattr(ravendb, "GetConversationMessagesOptions"))
        self.assertTrue(hasattr(ravendb, "AiMessageRole"))
        self.assertTrue(hasattr(ravendb, "AiConversationDetailLevel"))


class TestCancelPendingActionTools(unittest.TestCase):
    def _command(self, cancel=False, debug=None):
        operation = RunConversationOperation(
            agent_id="a",
            conversation_id="c",
            debug=debug,
            cancel_pending_action_tools=cancel,
        )
        return operation.get_command(None)

    def test_flag_always_on_wire_after_debug(self):
        url = self._command().create_request(ServerNode("http://localhost:8080", "db1")).url
        self.assertTrue(url.endswith("&cancelPendingActionTools=False"), url)

    def test_flag_after_debug_parameter(self):
        url = self._command(debug=True).create_request(ServerNode("http://localhost:8080", "db1")).url
        self.assertIn("&debug=True&cancelPendingActionTools=False", url)

    def test_flag_true_when_set(self):
        url = self._command(cancel=True).create_request(ServerNode("http://localhost:8080", "db1")).url
        self.assertTrue(url.endswith("&cancelPendingActionTools=True"), url)


class TestStreamingCancellation(unittest.TestCase):
    def test_cancelled_stream_stops_without_reading_rest(self):
        from ravendb.http.misc import ResponseDisposeHandling
        from ravendb.http.http_cache import HttpCache

        lines_read = []

        class FakeResponse:
            def iter_lines(self, decode_unicode=True):
                for i in range(100):
                    lines_read.append(i)
                    # Real streamed chunks are JSON-encoded strings, not bare objects.
                    yield f'"chunk {i}"'

            def close(self):
                pass

        event = threading.Event()
        command = RunConversationCommand(
            agent_id="a",
            conversation_id="c",
            stream_property_path="answer",
            streamed_chunks_callback=lambda chunk: event.set(),
            cancellation_event=event,
        )
        response = FakeResponse()
        with self.assertRaises(Exception) as ctx:
            command.process_response(HttpCache(), response, "http://x")
        self.assertIsInstance(ctx.exception, Exception)
        self.assertLess(len(lines_read), 100, "the stream must not be fully consumed")

    def test_no_cancellation_event_reads_to_end(self):
        from ravendb.http.misc import ResponseDisposeHandling
        from ravendb.http.http_cache import HttpCache

        class FakeResponse:
            def iter_lines(self, decode_unicode=True):
                yield '{"a": 1}'
                yield '{"a": 2}'

            def close(self):
                pass

        command = RunConversationCommand(
            agent_id="a",
            conversation_id="c",
            stream_property_path="answer",
            streamed_chunks_callback=lambda chunk: None,
        )
        handling = command.process_response(HttpCache(), FakeResponse(), "http://x")
        self.assertEqual(ResponseDisposeHandling.AUTOMATIC, handling)


if __name__ == "__main__":
    unittest.main()

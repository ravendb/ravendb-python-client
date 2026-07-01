"""
Unit tests for AiConversation messages API — GetConversationMessages.

Tests cover:
- AiConversationDetailLevel and AiMessageRole enums
- AiToolCallResult from_json deserialization
- Timestamp parsing (Z suffix, 7-digit fractions)
- Content parsing (multi-part arrays, object content)
- AiConversationMessage.from_json with tool calls, usage, roles
- AiConversationMessagesResult.from_json with parameter materialization
- GetConversationMessagesOptions validation (null, empty, whitespace, Before+After, PageSize)
- GetConversationMessagesOperation string/options overload
- GetConversationMessagesCommand URL creation and set_response
- _format_datetime_for_url correctness
"""

import json
import unittest
from datetime import datetime, timezone, timedelta

from ravendb.documents.operations.ai.agents import (
    AiConversationDetailLevel,
    AiMessageRole,
    AiToolCallResult,
    AiConversationMessage,
    AiConversationMessagesResult,
    GetConversationMessagesOptions,
    GetConversationMessagesOperation,
    AiUsage,
)
from ravendb.documents.operations.ai.agents.get_conversation_messages_operation import (
    GetConversationMessagesCommand,
    _format_datetime_for_url,
)


class TestAiConversationDetailLevel(unittest.TestCase):
    def test_enum_values(self):
        self.assertEqual(AiConversationDetailLevel.SIMPLE.value, "Simple")
        self.assertEqual(AiConversationDetailLevel.DETAILED.value, "Detailed")
        self.assertEqual(AiConversationDetailLevel.FULL.value, "Full")


class TestAiMessageRole(unittest.TestCase):
    def test_enum_values(self):
        self.assertEqual(AiMessageRole.SYSTEM.value, "System")
        self.assertEqual(AiMessageRole.USER.value, "User")
        self.assertEqual(AiMessageRole.ASSISTANT.value, "Assistant")
        self.assertEqual(AiMessageRole.SUMMARY.value, "Summary")
        self.assertEqual(AiMessageRole.INTERNAL.value, "Internal")

    def test_from_valid_string(self):
        self.assertEqual(AiMessageRole("System"), AiMessageRole.SYSTEM)
        self.assertEqual(AiMessageRole("User"), AiMessageRole.USER)
        self.assertEqual(AiMessageRole("Assistant"), AiMessageRole.ASSISTANT)
        self.assertEqual(AiMessageRole("Summary"), AiMessageRole.SUMMARY)
        self.assertEqual(AiMessageRole("Internal"), AiMessageRole.INTERNAL)

    def test_from_invalid_string_raises(self):
        with self.assertRaises(ValueError):
            AiMessageRole("Unknown")
        with self.assertRaises(ValueError):
            AiMessageRole("Tool")


class TestAiToolCallResult(unittest.TestCase):
    def test_from_json_full(self):
        data = {
            "Id": "call_abc123",
            "Name": "get_orders",
            "Arguments": '{"userId": "1"}',
            "Result": "Order #42",
            "SubConversationId": "SC/1",
        }
        result = AiToolCallResult.from_json(data)
        self.assertEqual(result.id, "call_abc123")
        self.assertEqual(result.name, "get_orders")
        self.assertEqual(result.arguments, '{"userId": "1"}')
        self.assertEqual(result.result, "Order #42")
        self.assertEqual(result.sub_conversation_id, "SC/1")

    def test_from_json_minimal(self):
        result = AiToolCallResult.from_json({"Id": "call_1", "Name": "get_orders"})
        self.assertEqual(result.id, "call_1")
        self.assertEqual(result.name, "get_orders")
        self.assertIsNone(result.arguments)

    def test_from_json_empty(self):
        result = AiToolCallResult.from_json({})
        self.assertIsNone(result.id)
        self.assertIsNone(result.name)

    def test_round_trip(self):
        original = AiToolCallResult(
            id="call_123",
            name="get_weather",
            arguments='{"city": "London"}',
            result="Sunny, 22C",
            sub_conversation_id="sub/1",
        )
        json_dict = original.to_json()
        restored = AiToolCallResult.from_json(json_dict)
        self.assertEqual(original.id, restored.id)
        self.assertEqual(original.name, restored.name)
        self.assertEqual(original.arguments, restored.arguments)
        self.assertEqual(original.result, restored.result)
        self.assertEqual(original.sub_conversation_id, restored.sub_conversation_id)


class TestTimestampParsing(unittest.TestCase):
    def _parse_msg(self, ts_str):
        raw = json.dumps({"Role": "User", "Content": "hi", "Timestamp": ts_str})
        return AiConversationMessage.from_json(json.loads(raw))

    def test_z_suffix(self):
        msg = self._parse_msg("2025-03-20T12:34:56.7890123Z")
        self.assertIsNotNone(msg.timestamp)
        self.assertEqual(msg.timestamp.year, 2025)
        self.assertEqual(msg.timestamp.month, 3)
        self.assertEqual(msg.timestamp.day, 20)
        self.assertEqual(msg.timestamp.hour, 12)
        self.assertEqual(msg.timestamp.minute, 34)
        self.assertEqual(msg.timestamp.second, 56)
        self.assertEqual(msg.timestamp.tzinfo, timezone.utc)

    def test_z_suffix_7_digit_fraction(self):
        """7-digit fractional seconds are truncated to 6 digits for Python 3.9."""
        msg = self._parse_msg("2025-03-20T12:34:56.7890123Z")
        self.assertEqual(msg.timestamp.microsecond, 789012)

    def test_z_suffix_6_digit_fraction(self):
        msg = self._parse_msg("2025-03-20T12:34:56.123456Z")
        self.assertEqual(msg.timestamp.microsecond, 123456)

    def test_no_timestamp(self):
        raw = json.dumps({"Role": "User", "Content": "hi"})
        msg = AiConversationMessage.from_json(json.loads(raw))
        self.assertIsNone(msg.timestamp)

    def test_null_timestamp(self):
        raw = json.dumps({"Role": "User", "Content": "hi", "Timestamp": None})
        msg = AiConversationMessage.from_json(json.loads(raw))
        self.assertIsNone(msg.timestamp)


class TestMessageFromJson(unittest.TestCase):
    def test_role_fail_fast_unknown(self):
        """Unknown roles must raise ValueError, not silently default."""
        raw = json.dumps({"Role": "Tool", "Content": "test", "Timestamp": "2025-01-01T00:00:00Z"})
        with self.assertRaises(ValueError):
            AiConversationMessage.from_json(json.loads(raw))

    def test_role_missing_raises(self):
        raw = json.dumps({"Content": "test", "Timestamp": "2025-01-01T00:00:00Z"})
        with self.assertRaises(ValueError):
            AiConversationMessage.from_json(json.loads(raw))

    def test_content_passthrough(self):
        """Content passes through from server as-is — normalization is server-side."""
        raw = json.dumps({"Role": "User", "Content": "Hello!", "Timestamp": "2025-01-01T00:00:00Z"})
        msg = AiConversationMessage.from_json(json.loads(raw))
        self.assertEqual(msg.content, "Hello!")

    def test_none_content(self):
        raw = json.dumps({"Role": "User", "Content": None, "Timestamp": "2025-01-01T00:00:00Z"})
        msg = AiConversationMessage.from_json(json.loads(raw))
        self.assertIsNone(msg.content)

    def test_user_message(self):
        raw = json.dumps(
            {"Role": "User", "Content": "Hello", "Timestamp": "2025-01-01T00:00:00Z", "Attachments": ["file1.pdf"]}
        )
        msg = AiConversationMessage.from_json(json.loads(raw))
        self.assertEqual(msg.role, AiMessageRole.USER)
        self.assertEqual(msg.content, "Hello")
        self.assertEqual(msg.attachments, ["file1.pdf"])

    def test_assistant_with_tool_calls(self):
        raw = json.dumps(
            {
                "Role": "Assistant",
                "Content": None,
                "Timestamp": "2025-01-01T00:00:00Z",
                "ToolCalls": [{"Id": "call_1", "Name": "get_orders", "Arguments": "{}", "Result": "done"}],
            }
        )
        msg = AiConversationMessage.from_json(json.loads(raw))
        self.assertEqual(msg.role, AiMessageRole.ASSISTANT)
        self.assertIsNone(msg.content)
        self.assertEqual(len(msg.tool_calls), 1)
        self.assertEqual(msg.tool_calls[0].name, "get_orders")
        self.assertEqual(msg.tool_calls[0].result, "done")

    def test_assistant_with_usage(self):
        raw = json.dumps(
            {
                "Role": "Assistant",
                "Content": "Hello",
                "Timestamp": "2025-01-01T00:00:00Z",
                "Usage": {
                    "PromptTokens": 10,
                    "CompletionTokens": 20,
                    "TotalTokens": 30,
                    "CachedTokens": 0,
                    "ReasoningTokens": 5,
                },
            }
        )
        msg = AiConversationMessage.from_json(json.loads(raw))
        self.assertIsNotNone(msg.usage)
        self.assertEqual(msg.usage.prompt_tokens, 10)
        self.assertEqual(msg.usage.reasoning_tokens, 5)

    def test_internal_with_sub_conversation_id(self):
        raw = json.dumps(
            {
                "Role": "Internal",
                "Content": "internal msg",
                "Timestamp": "2025-01-01T00:00:00Z",
                "SubConversationId": "SC/1",
            }
        )
        msg = AiConversationMessage.from_json(json.loads(raw))
        self.assertEqual(msg.role, AiMessageRole.INTERNAL)
        self.assertEqual(msg.sub_conversation_id, "SC/1")

    def test_summary_role(self):
        raw = json.dumps({"Role": "Summary", "Content": "Summary", "Timestamp": "2025-01-01T00:00:00Z"})
        msg = AiConversationMessage.from_json(json.loads(raw))
        self.assertEqual(msg.role, AiMessageRole.SUMMARY)

    def test_tool_calls_empty_list_by_default(self):
        msg = AiConversationMessage(role=AiMessageRole.USER, content="test")
        self.assertIsNone(msg.tool_calls)

    def test_attachments_empty_list_by_default(self):
        msg = AiConversationMessage(role=AiMessageRole.USER, content="test")
        self.assertIsNone(msg.attachments)

    def test_round_trip(self):
        timestamp = datetime(2025, 3, 20, 12, 34, 56, 789012, tzinfo=timezone.utc)
        msg = AiConversationMessage(
            role=AiMessageRole.USER, content="Hello", attachments=["f.pdf"], timestamp=timestamp
        )
        restored = AiConversationMessage.from_json(msg.to_json())
        self.assertEqual(restored.role, AiMessageRole.USER)
        self.assertEqual(restored.content, "Hello")
        self.assertEqual(restored.attachments, ["f.pdf"])


class TestGetConversationMessagesOptions(unittest.TestCase):
    def test_defaults(self):
        opts = GetConversationMessagesOptions(conversation_id="Chats/1-A")
        self.assertEqual(opts.conversation_id, "Chats/1-A")
        self.assertIsNone(opts.before)
        self.assertIsNone(opts.after)
        self.assertEqual(opts.page_size, 2147483647)
        self.assertEqual(opts.detail_level, AiConversationDetailLevel.SIMPLE)

    def test_validate_null(self):
        with self.assertRaises(ValueError):
            GetConversationMessagesOptions(conversation_id=None).validate()

    def test_validate_empty(self):
        with self.assertRaises(ValueError):
            GetConversationMessagesOptions(conversation_id="").validate()

    def test_validate_whitespace(self):
        with self.assertRaises(ValueError):
            GetConversationMessagesOptions(conversation_id="   ").validate()

    def test_validate_before_and_after(self):
        opts = GetConversationMessagesOptions(
            conversation_id="Chats/1-A",
            before=datetime.now(timezone.utc),
            after=datetime.now(timezone.utc),
        )
        with self.assertRaises(ValueError):
            opts.validate()

    def test_validate_page_size_zero(self):
        with self.assertRaises(ValueError):
            GetConversationMessagesOptions(conversation_id="Chats/1-A", page_size=0).validate()

    def test_validate_page_size_negative(self):
        with self.assertRaises(ValueError):
            GetConversationMessagesOptions(conversation_id="Chats/1-A", page_size=-5).validate()

    def test_page_size_default_is_large(self):
        opts = GetConversationMessagesOptions(conversation_id="Chats/1-A")
        self.assertEqual(opts.page_size, 2147483647)

    def test_detail_level_default_is_simple(self):
        opts = GetConversationMessagesOptions(conversation_id="Chats/1-A")
        self.assertEqual(opts.detail_level, AiConversationDetailLevel.SIMPLE)


class TestGetConversationMessagesOperation(unittest.TestCase):
    def test_from_string(self):
        op = GetConversationMessagesOperation("Chats/1-A")
        self.assertEqual(op._parameters.conversation_id, "Chats/1-A")

    def test_from_options(self):
        opts = GetConversationMessagesOptions(
            conversation_id="Chats/1-A", detail_level=AiConversationDetailLevel.DETAILED, page_size=50
        )
        op = GetConversationMessagesOperation(opts)
        self.assertEqual(op._parameters.detail_level, AiConversationDetailLevel.DETAILED)
        self.assertEqual(op._parameters.page_size, 50)

    def test_from_invalid_type_raises(self):
        with self.assertRaises(TypeError):
            GetConversationMessagesOperation(42)

    def test_validates_on_construction(self):
        with self.assertRaises(ValueError):
            GetConversationMessagesOperation("")

    def test_get_command_is_read_request(self):
        from ravendb.documents.conventions import DocumentConventions

        op = GetConversationMessagesOperation("Chats/1-A")
        cmd = op.get_command(DocumentConventions())
        self.assertTrue(cmd.is_read_request())


class TestFormatDatetimeForUrl(unittest.TestCase):
    def test_utc_datetime(self):
        dt = datetime(2025, 3, 20, 12, 34, 56, 789012, tzinfo=timezone.utc)
        formatted = _format_datetime_for_url(dt)
        self.assertTrue(formatted.endswith("Z"))
        self.assertIn("2025-03-20T12:34:56.7890120Z", formatted)

    def test_naive_datetime(self):
        dt = datetime(2025, 3, 20, 12, 34, 56, 123456)
        formatted = _format_datetime_for_url(dt)
        self.assertTrue(formatted.endswith("Z"))
        self.assertIn("2025-03-20T12:34:56.1234560Z", formatted)

    def test_non_utc_timezone(self):
        dt = datetime(2025, 3, 20, 12, 34, 56, 0, tzinfo=timezone(timedelta(hours=5)))
        formatted = _format_datetime_for_url(dt)
        self.assertIn("07:34:56", formatted)
        self.assertTrue(formatted.endswith("Z"))


class TestCommandSetResponse(unittest.TestCase):
    def test_null_response_is_404(self):
        opts = GetConversationMessagesOptions(conversation_id="Chats/1-A")
        cmd = GetConversationMessagesCommand(opts)
        cmd.set_response(None, False)
        self.assertIsNone(cmd.result)

    def test_response_with_messages(self):
        server_json = {
            "ConversationId": "Chats/1-A",
            "Agent": "my-agent",
            "Parameters": {"budget": 3500},
            "TotalUsage": {
                "PromptTokens": 10,
                "CompletionTokens": 20,
                "TotalTokens": 30,
                "CachedTokens": 0,
                "ReasoningTokens": 0,
            },
            "LastMessageAt": "2025-03-20T12:34:56.7890123Z",
            "Messages": [
                {"Role": "User", "Content": "Hello", "Timestamp": "2025-03-20T12:34:56.123456Z"},
                {
                    "Role": "Assistant",
                    "Content": "Hi!",
                    "Timestamp": "2025-03-20T12:34:57.000000Z",
                    "Usage": {
                        "PromptTokens": 5,
                        "CompletionTokens": 10,
                        "TotalTokens": 15,
                        "CachedTokens": 0,
                        "ReasoningTokens": 2,
                    },
                },
            ],
            "HasMoreMessages": False,
            "SubConversationIds": [],
            "Attachments": ["file1.pdf"],
        }
        opts = GetConversationMessagesOptions(conversation_id="Chats/1-A")
        cmd = GetConversationMessagesCommand(opts)
        cmd.set_response(json.dumps(server_json), False)
        result = cmd.result
        self.assertIsNotNone(result)
        self.assertEqual(result.conversation_id, "Chats/1-A")
        self.assertEqual(result.agent, "my-agent")
        self.assertEqual(len(result.messages), 2)
        self.assertEqual(result.messages[0].role, AiMessageRole.USER)
        self.assertEqual(result.messages[1].role, AiMessageRole.ASSISTANT)
        self.assertIsNotNone(result.total_usage)
        self.assertEqual(result.total_usage.prompt_tokens, 10)
        self.assertEqual(result.attachments, ["file1.pdf"])

    def test_has_more_messages(self):
        server_json = {
            "ConversationId": "Chats/1-A",
            "Agent": "agent",
            "Parameters": {},
            "Messages": [{"Role": "User", "Content": "Hi", "Timestamp": "2025-01-01T00:00:00Z"}],
            "HasMoreMessages": True,
        }
        opts = GetConversationMessagesOptions(conversation_id="Chats/1-A")
        cmd = GetConversationMessagesCommand(opts)
        cmd.set_response(json.dumps(server_json), False)
        self.assertTrue(cmd.result.has_more_messages)

    def test_empty_messages_no_crash(self):
        server_json = {
            "ConversationId": "Chats/1-A",
            "Agent": "agent",
            "Messages": [],
            "HasMoreMessages": False,
        }
        opts = GetConversationMessagesOptions(conversation_id="Chats/1-A")
        cmd = GetConversationMessagesCommand(opts)
        cmd.set_response(json.dumps(server_json), False)
        self.assertEqual(len(cmd.result.messages), 0)


class TestParameterMaterialization(unittest.TestCase):
    def _parse_result(self, params_dict):
        return AiConversationMessagesResult.from_json(
            {
                "ConversationId": "Chats/1-A",
                "Agent": "agent",
                "Parameters": params_dict,
                "Messages": [{"Role": "User", "Content": "hi", "Timestamp": "2025-01-01T00:00:00Z"}],
            }
        )

    def test_string_parameter(self):
        result = self._parse_result({"name": "test-agent"})
        self.assertEqual(result.parameters["name"], "test-agent")
        self.assertIsInstance(result.parameters["name"], str)

    def test_integer_parameter(self):
        result = self._parse_result({"budget": 3500})
        self.assertEqual(result.parameters["budget"], 3500)
        self.assertIsInstance(result.parameters["budget"], int)

    def test_float_parameter(self):
        result = self._parse_result({"price": 19.99})
        self.assertEqual(result.parameters["price"], 19.99)
        self.assertIsInstance(result.parameters["price"], float)

    def test_bool_parameter(self):
        result = self._parse_result({"enabled": True})
        self.assertEqual(result.parameters["enabled"], True)
        self.assertIsInstance(result.parameters["enabled"], bool)

    def test_null_parameter(self):
        result = self._parse_result({"value": None})
        self.assertIsNone(result.parameters["value"])

    def test_empty_parameters(self):
        result = self._parse_result({})
        self.assertEqual(len(result.parameters), 0)

    def test_no_parameters_key(self):
        result = AiConversationMessagesResult.from_json(
            {
                "ConversationId": "Chats/1-A",
                "Agent": "agent",
                "Messages": [{"Role": "User", "Content": "hi", "Timestamp": "2025-01-01T00:00:00Z"}],
            }
        )
        self.assertEqual(len(result.parameters), 0)

    def test_string_array_parameter(self):
        result = self._parse_result({"tags": ["a", "b", "c"]})
        self.assertEqual(result.parameters["tags"], ["a", "b", "c"])
        self.assertIsInstance(result.parameters["tags"][0], str)

    def test_int_array_parameter(self):
        result = self._parse_result({"scores": [1, 2, 3]})
        self.assertEqual(result.parameters["scores"], [1, 2, 3])

    def test_float_array_promotion(self):
        result = self._parse_result({"values": [1, 1.5, 2]})
        for v in result.parameters["values"]:
            self.assertIsInstance(v, float)

    def test_bool_array_parameter(self):
        result = self._parse_result({"flags": [True, False, True]})
        self.assertEqual(result.parameters["flags"], [True, False, True])

    def test_mixed_array(self):
        result = self._parse_result({"mixed": [1, "two", 3.0]})
        self.assertEqual(len(result.parameters["mixed"]), 3)

    def test_parameters_default_to_empty_dict(self):
        result = AiConversationMessagesResult()
        self.assertEqual(result.parameters, {})

    def test_messages_default_to_empty_list(self):
        result = AiConversationMessagesResult()
        self.assertEqual(result.messages, [])

    def test_sub_conversation_ids_default_to_empty_list(self):
        result = AiConversationMessagesResult()
        self.assertEqual(result.sub_conversation_ids, [])

    def test_attachments_default_to_empty_list(self):
        result = AiConversationMessagesResult()
        self.assertEqual(result.attachments, [])


class TestCreateRequest(unittest.TestCase):
    def setUp(self):
        from ravendb.http.server_node import ServerNode

        self.node = ServerNode(url="http://localhost:8080", database="testdb", cluster_tag=None)

    def test_basic_url(self):
        opts = GetConversationMessagesOptions(conversation_id="Chats/1-A")
        cmd = GetConversationMessagesCommand(opts)
        req = cmd.create_request(self.node)
        self.assertIn("conversationId=Chats%2F1-A", req.url)
        self.assertIn("pageSize=2147483647", req.url)
        self.assertIn("detailLevel=Simple", req.url)
        self.assertNotIn("before", req.url)
        self.assertNotIn("after", req.url)

    def test_before_url(self):
        dt = datetime(2025, 3, 20, 12, 34, 56, 789012, tzinfo=timezone.utc)
        opts = GetConversationMessagesOptions(conversation_id="Chats/1-A", before=dt)
        cmd = GetConversationMessagesCommand(opts)
        req = cmd.create_request(self.node)
        self.assertIn("before=", req.url)
        self.assertNotIn("after=", req.url)

    def test_after_url(self):
        dt = datetime(2025, 3, 20, 12, 34, 56, 789012, tzinfo=timezone.utc)
        opts = GetConversationMessagesOptions(conversation_id="Chats/1-A", after=dt)
        cmd = GetConversationMessagesCommand(opts)
        req = cmd.create_request(self.node)
        self.assertIn("after=", req.url)

    def test_page_size_url(self):
        opts = GetConversationMessagesOptions(conversation_id="Chats/1-A", page_size=20)
        cmd = GetConversationMessagesCommand(opts)
        req = cmd.create_request(self.node)
        self.assertIn("pageSize=20", req.url)

    def test_detail_level_detailed(self):
        opts = GetConversationMessagesOptions(
            conversation_id="Chats/1-A", detail_level=AiConversationDetailLevel.DETAILED
        )
        cmd = GetConversationMessagesCommand(opts)
        req = cmd.create_request(self.node)
        self.assertIn("detailLevel=Detailed", req.url)


class TestFullResultDeserialization(unittest.TestCase):
    """End-to-end test of the full server response deserialization."""

    def test_complex_response(self):
        server_json = {
            "ConversationId": "Chats/1-A",
            "Agent": "test-agent",
            "Parameters": {
                "string_param": "hello",
                "int_param": 42,
                "float_param": 3.14,
                "bool_param": True,
                "null_param": None,
                "string_list": ["a", "b"],
                "int_list": [1, 2, 3],
                "bool_list": [True, False],
            },
            "TotalUsage": {
                "PromptTokens": 100,
                "CompletionTokens": 200,
                "TotalTokens": 300,
                "CachedTokens": 10,
                "ReasoningTokens": 50,
            },
            "LastMessageAt": "2025-06-15T10:30:00.1234567Z",
            "Messages": [
                {
                    "Role": "System",
                    "Content": "You are a helpful assistant.",
                    "Timestamp": "2025-06-15T10:29:00.000000Z",
                },
                {"Role": "User", "Content": "Hello!", "Timestamp": "2025-06-15T10:30:00.000000Z"},
                {
                    "Role": "Assistant",
                    "Content": None,
                    "Timestamp": "2025-06-15T10:30:01.000000Z",
                    "ToolCalls": [
                        {
                            "Id": "call_1",
                            "Name": "get_orders",
                            "Arguments": "{}",
                            "Result": "Order #42",
                            "SubConversationId": None,
                        }
                    ],
                    "Usage": {
                        "PromptTokens": 50,
                        "CompletionTokens": 100,
                        "TotalTokens": 150,
                        "CachedTokens": 0,
                        "ReasoningTokens": 20,
                    },
                },
                {"Role": "Summary", "Content": "Conversation summarized.", "Timestamp": "2025-06-15T10:31:00.000000Z"},
            ],
            "HasMoreMessages": False,
            "SubConversationIds": ["SC/1"],
            "Attachments": ["report.pdf", "data.csv"],
        }
        result = AiConversationMessagesResult.from_json(server_json)
        self.assertEqual(result.conversation_id, "Chats/1-A")
        self.assertEqual(result.agent, "test-agent")
        self.assertEqual(len(result.parameters), 8)
        self.assertEqual(result.parameters["string_param"], "hello")
        self.assertEqual(result.parameters["int_param"], 42)
        self.assertEqual(result.parameters["float_param"], 3.14)
        self.assertEqual(result.parameters["bool_param"], True)
        self.assertIsNone(result.parameters["null_param"])
        self.assertEqual(result.parameters["string_list"], ["a", "b"])
        self.assertEqual(result.parameters["int_list"], [1, 2, 3])
        self.assertEqual(result.parameters["bool_list"], [True, False])

        self.assertEqual(result.total_usage.prompt_tokens, 100)
        self.assertEqual(result.total_usage.reasoning_tokens, 50)
        self.assertIsNotNone(result.last_message_at)
        self.assertEqual(result.last_message_at.year, 2025)

        self.assertEqual(len(result.messages), 4)
        self.assertEqual(result.messages[0].role, AiMessageRole.SYSTEM)
        self.assertEqual(result.messages[1].role, AiMessageRole.USER)
        self.assertEqual(result.messages[2].role, AiMessageRole.ASSISTANT)
        self.assertEqual(result.messages[3].role, AiMessageRole.SUMMARY)

        # Tool calls on assistant message
        self.assertEqual(len(result.messages[2].tool_calls), 1)
        self.assertEqual(result.messages[2].tool_calls[0].name, "get_orders")
        self.assertEqual(result.messages[2].tool_calls[0].result, "Order #42")

        self.assertEqual(result.sub_conversation_ids, ["SC/1"])
        self.assertEqual(result.attachments, ["report.pdf", "data.csv"])


class TestAiOperationsIntegration(unittest.TestCase):
    def test_method_exists_on_ai_operations(self):
        from ravendb.documents.ai.ai_operations import AiOperations

        self.assertTrue(hasattr(AiOperations, "get_conversation_messages"))
        self.assertTrue(callable(AiOperations.get_conversation_messages))


if __name__ == "__main__":
    unittest.main()

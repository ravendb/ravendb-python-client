"""
Tests for GetConversationMessagesOperation, the 7.2.5 way to read an AI agent
conversation back, and for the cancel-pending-action-tools flag on a run.
"""

import json
import unittest
from datetime import datetime, timedelta, timezone

from ravendb.documents.operations.ai.agents import (
    AiConversationDetailLevel,
    AiConversationMessage,
    AiConversationMessagesResult,
    AiMessageRole,
    AiToolCallResult,
    AiUsage,
    GetConversationMessagesOperation,
    GetConversationMessagesOptions,
    RunConversationOperation,
)
from ravendb.http.server_node import ServerNode
from ravendb.primitives import constants


class TestGetConversationMessagesOptions(unittest.TestCase):
    def test_defaults_to_the_whole_conversation_at_simple_detail(self):
        options = GetConversationMessagesOptions("chats/1-A")

        self.assertEqual(constants.int_max, options.page_size)
        self.assertEqual(AiConversationDetailLevel.SIMPLE, options.detail_level)
        self.assertIsNone(options.before)
        self.assertIsNone(options.after)

    def test_a_conversation_id_is_required(self):
        with self.assertRaises(ValueError):
            GetConversationMessagesOperation("")
        with self.assertRaises(ValueError):
            GetConversationMessagesOperation(GetConversationMessagesOptions(None))
        with self.assertRaises(ValueError):
            GetConversationMessagesOperation(None)

    def test_paging_backwards_and_forwards_at_once_is_rejected(self):
        options = GetConversationMessagesOptions("chats/1-A", before=datetime(2026, 6, 16), after=datetime(2026, 6, 15))

        with self.assertRaises(ValueError):
            GetConversationMessagesOperation(options)

    def test_a_non_positive_page_size_is_rejected(self):
        with self.assertRaises(ValueError):
            GetConversationMessagesOperation(GetConversationMessagesOptions("chats/1-A", page_size=0))
        with self.assertRaises(ValueError):
            GetConversationMessagesOperation(GetConversationMessagesOptions("chats/1-A", page_size=-1))


class TestGetConversationMessagesCommand(unittest.TestCase):
    def setUp(self):
        self.node = ServerNode("http://localhost:8080", "db")

    def _url(self, conversation_id_or_options):
        command = GetConversationMessagesOperation(conversation_id_or_options).get_command(None)
        return command.create_request(self.node).url

    def test_the_conversation_id_is_escaped_into_the_query_string(self):
        url = self._url("chats/1-A")

        self.assertTrue(
            url.startswith("http://localhost:8080/databases/db/ai/agent/conversation/messages?conversationId="),
            url,
        )
        self.assertIn("conversationId=chats%2F1-A", url)

    def test_page_size_and_detail_level_are_always_sent(self):
        url = self._url(GetConversationMessagesOptions("chats/1-A", page_size=25))

        self.assertIn("&pageSize=25", url)
        self.assertIn("&detailLevel=Simple", url)

    def test_detail_level_is_sent_by_its_server_side_name(self):
        url = self._url(GetConversationMessagesOptions("chats/1-A", detail_level=AiConversationDetailLevel.FULL))

        self.assertIn("&detailLevel=Full", url)

    def test_a_naive_cursor_is_sent_as_is(self):
        # Naive datetimes count as UTC here, the same rule the rest of the client follows.
        url = self._url(GetConversationMessagesOptions("chats/1-A", before=datetime(2026, 6, 16, 10, 30, 0)))

        self.assertIn("&before=2026-06-16T10%3A30%3A00.0000000", url)

    def test_an_aware_cursor_is_converted_to_utc(self):
        warsaw = datetime(2026, 6, 16, 10, 30, 0, tzinfo=timezone(timedelta(hours=2)))
        url = self._url(GetConversationMessagesOptions("chats/1-A", after=warsaw))

        self.assertIn("&after=2026-06-16T08%3A30%3A00.0000000", url)

    def test_reading_messages_is_a_read_request(self):
        command = GetConversationMessagesOperation("chats/1-A").get_command(None)

        self.assertTrue(command.is_read_request())

    def test_a_missing_conversation_leaves_the_result_unset(self):
        # The endpoint answers 404 for an unknown conversation, which reaches us as None.
        command = GetConversationMessagesOperation("chats/1-A").get_command(None)
        command.set_response(None, False)

        self.assertIsNone(command.result)


class TestAiConversationMessagesResult(unittest.TestCase):
    RESPONSE = {
        "ConversationId": "chats/1-A",
        "Agent": "agents/support",
        "Parameters": {"customer": "ALFKI", "tags": ["vip", "eu"], "retries": 2},
        "TotalUsage": {"PromptTokens": 10, "CompletionTokens": 5, "TotalTokens": 15, "ReasoningTokens": 3},
        "LastMessageAt": "2026-06-16T10:30:05.0000000",
        "HasMoreMessages": True,
        "SubConversationIds": ["chats/1-A/sub/1"],
        "Attachments": ["invoice.pdf"],
        "Messages": [
            {"Role": "User", "Content": "where is my order?", "Timestamp": "2026-06-16T10:30:00.0000000"},
            {
                "Role": "Assistant",
                "Content": None,
                "Timestamp": "2026-06-16T10:30:05.0000000",
                "Usage": {"PromptTokens": 10, "CompletionTokens": 5, "TotalTokens": 15},
                "ToolCalls": [
                    {
                        "Id": "call_1",
                        "Name": "lookup_order",
                        "Arguments": '{"id":"orders/1-A"}',
                        "Result": '{"status":"Shipped"}',
                        "SubConversationId": "chats/1-A/sub/1",
                    }
                ],
            },
        ],
    }

    def test_result_parses_the_conversation_envelope(self):
        result = AiConversationMessagesResult.from_json(self.RESPONSE)

        self.assertEqual("chats/1-A", result.conversation_id)
        self.assertEqual("agents/support", result.agent)
        self.assertTrue(result.has_more_messages)
        self.assertEqual(["chats/1-A/sub/1"], result.sub_conversation_ids)
        self.assertEqual(["invoice.pdf"], result.attachments)
        self.assertEqual(datetime(2026, 6, 16, 10, 30, 5), result.last_message_at)

    def test_heterogeneous_parameters_come_back_as_they_are(self):
        # Parameter values mix primitives and arrays, so they are handed over untouched.
        parameters = AiConversationMessagesResult.from_json(self.RESPONSE).parameters

        self.assertEqual({"customer": "ALFKI", "tags": ["vip", "eu"], "retries": 2}, parameters)

    def test_usage_includes_reasoning_tokens(self):
        result = AiConversationMessagesResult.from_json(self.RESPONSE)

        self.assertIsInstance(result.total_usage, AiUsage)
        self.assertEqual(3, result.total_usage.reasoning_tokens)
        self.assertEqual(15, result.total_usage.total_tokens)

    def test_messages_keep_their_role_and_timestamp(self):
        messages = AiConversationMessagesResult.from_json(self.RESPONSE).messages

        self.assertEqual(2, len(messages))
        self.assertEqual(AiMessageRole.USER, messages[0].role)
        self.assertEqual(AiMessageRole.ASSISTANT, messages[1].role)
        self.assertEqual(datetime(2026, 6, 16, 10, 30, 0), messages[0].timestamp)

    def test_tool_calls_carry_their_result_and_sub_conversation(self):
        tool_call = AiConversationMessagesResult.from_json(self.RESPONSE).messages[1].tool_calls[0]

        self.assertIsInstance(tool_call, AiToolCallResult)
        self.assertEqual("lookup_order", tool_call.name)
        self.assertEqual('{"status":"Shipped"}', tool_call.result)
        self.assertEqual("chats/1-A/sub/1", tool_call.sub_conversation_id)

    def test_an_assistant_message_that_only_called_tools_has_no_content(self):
        messages = AiConversationMessagesResult.from_json(self.RESPONSE).messages

        self.assertIsNone(messages[1].content)

    def test_result_survives_a_json_round_trip(self):
        result = AiConversationMessagesResult.from_json(self.RESPONSE)
        round_tripped = AiConversationMessagesResult.from_json(result.to_json())

        self.assertEqual(result.to_json(), round_tripped.to_json())

    def test_a_pending_tool_call_has_no_result_yet(self):
        tool_call = AiToolCallResult.from_json({"Id": "call_1", "Name": "lookup_order", "Arguments": "{}"})

        self.assertIsNone(tool_call.result)

    def test_an_empty_message_list_stays_empty(self):
        result = AiConversationMessagesResult.from_json({"ConversationId": "chats/1-A", "Messages": []})

        self.assertIsNone(result.messages)
        self.assertFalse(result.has_more_messages)

    def test_message_to_json_writes_the_role_by_name(self):
        message = AiConversationMessage(role=AiMessageRole.SUMMARY, content="short")

        self.assertEqual("Summary", message.to_json()["Role"])


class TestCancelPendingActionTools(unittest.TestCase):
    def setUp(self):
        self.node = ServerNode("http://localhost:8080", "db")

    def _url(self, **kwargs):
        operation = RunConversationOperation(agent_id="agents/support", conversation_id="chats/1-A", **kwargs)
        return operation.get_command(None).create_request(self.node).url

    def test_the_flag_is_sent_on_every_run(self):
        # The server has no default of its own, so the client always states its intent.
        self.assertIn("&cancelPendingActionTools=False", self._url())

    def test_the_flag_is_sent_when_it_is_set(self):
        self.assertIn("&cancelPendingActionTools=True", self._url(cancel_pending_action_tools=True))

"""AI conversation messages tests, ported from AiAgentGetConversationMessages."""

import os
import unittest
from datetime import datetime

from ravendb.documents.operations.ai.agents import (
    AiConversationDetailLevel,
    AiConversationMessage,
    AiConversationMessagesResult,
    AiMessageRole,
    AiToolCallResult,
    GetConversationMessagesOperation,
    GetConversationMessagesOptions,
)
from ravendb.http.server_node import ServerNode
from ravendb.tests.test_base import TestBase


class TestGetConversationMessagesWireShape(unittest.TestCase):
    def _url(self, options: GetConversationMessagesOptions) -> str:
        operation = GetConversationMessagesOperation(options)
        command = operation.get_command(None)
        request = command.create_request(ServerNode("http://localhost:8080", "db"))
        return request.url

    def test_url_has_all_parameters(self):
        url = self._url(GetConversationMessagesOptions(conversation_id="conversations/1"))
        self.assertEqual(
            url,
            "http://localhost:8080/databases/db/ai/agent/conversation/messages"
            "?conversationId=conversations/1&pageSize=2147483647&detailLevel=Simple",
        )

    def test_operation_accepts_plain_conversation_id(self):
        # The string form must build the same URL as the options form with defaults.
        request = (
            GetConversationMessagesOperation("conversations/1")
            .get_command(None)
            .create_request(ServerNode("http://localhost:8080", "db"))
        )
        self.assertEqual(
            request.url,
            "http://localhost:8080/databases/db/ai/agent/conversation/messages"
            "?conversationId=conversations/1&pageSize=2147483647&detailLevel=Simple",
        )

    def test_url_includes_before_and_detail_level(self):
        options = GetConversationMessagesOptions(
            conversation_id="conversations/1",
            before=datetime(2026, 6, 1, 12, 0, 0),
            page_size=50,
            detail_level=AiConversationDetailLevel.DETAILED,
        )
        url = self._url(options)
        self.assertIn("&before=2026-06-01T12%3A00%3A00.0000000", url)
        self.assertIn("&pageSize=50", url)
        self.assertIn("&detailLevel=Detailed", url)

    def test_validation(self):
        with self.assertRaises(ValueError):
            GetConversationMessagesOperation(GetConversationMessagesOptions())
        with self.assertRaises(ValueError):
            GetConversationMessagesOperation(
                GetConversationMessagesOptions(
                    conversation_id="conversations/1",
                    before=datetime(2026, 1, 1),
                    after=datetime(2026, 1, 2),
                )
            )
        with self.assertRaises(ValueError):
            GetConversationMessagesOperation(GetConversationMessagesOptions(conversation_id="c", page_size=0))


class TestGetConversationMessagesResponseParse(unittest.TestCase):
    """Response parsing, pinned to the reference AiConversationMessagesResult wire."""

    def _sample_payload(self):
        return {
            "ConversationId": "conversations/1",
            "Agent": "agents/1",
            "Parameters": {"temperature": 0.5},
            "TotalUsage": {
                "PromptTokens": 10,
                "CompletionTokens": 20,
                "TotalTokens": 30,
                "CachedTokens": 0,
                "ReasoningTokens": 5,
            },
            "LastMessageAt": "2026-06-01T12:00:00.0000000",
            "Messages": [
                {
                    "Role": "User",
                    "Content": "hello",
                    "Attachments": ["a.txt"],
                    "Timestamp": "2026-06-01T11:59:00.0000000",
                    "ToolCalls": None,
                    "Usage": None,
                    "SubConversationId": None,
                },
                {
                    "Role": "Assistant",
                    "Content": "hi",
                    "Attachments": [],
                    "Timestamp": "2026-06-01T12:00:00.0000000",
                    "ToolCalls": [
                        {
                            "Id": "call-1",
                            "Name": "get_weather",
                            "Arguments": "{}",
                            "Result": "sunny",
                            "SubConversationId": None,
                        }
                    ],
                    "Usage": {
                        "PromptTokens": 1,
                        "CompletionTokens": 2,
                        "TotalTokens": 3,
                        "CachedTokens": 0,
                        "ReasoningTokens": 0,
                    },
                    "SubConversationId": None,
                },
            ],
            "HasMoreMessages": True,
            "SubConversationIds": ["conversations/1/sub/1"],
            "Attachments": ["b.txt"],
        }

    def test_result_from_json_binds_all_fields(self):
        result = AiConversationMessagesResult.from_json(self._sample_payload())
        self.assertEqual("conversations/1", result.conversation_id)
        self.assertEqual("agents/1", result.agent)
        self.assertTrue(result.has_more_messages)
        self.assertEqual(["conversations/1/sub/1"], result.sub_conversation_ids)
        self.assertEqual(["b.txt"], result.attachments)
        self.assertEqual(2, len(result.messages))
        self.assertEqual(datetime(2026, 6, 1, 12, 0, 0), result.last_message_at)
        self.assertEqual(0.5, result.parameters["temperature"])

    def test_message_from_json_binds_role_content_and_tool_calls(self):
        result = AiConversationMessagesResult.from_json(self._sample_payload())
        messages = result.messages
        first = messages[0]
        self.assertEqual(AiMessageRole.USER, first.role)
        self.assertEqual("hello", first.content)
        self.assertEqual(["a.txt"], first.attachments)
        self.assertEqual([], first.tool_calls)

        second = messages[1]
        self.assertEqual(AiMessageRole.ASSISTANT, second.role)
        self.assertEqual(1, len(second.tool_calls))
        tool_call = second.tool_calls[0]
        self.assertIsInstance(tool_call, AiToolCallResult)
        self.assertEqual("call-1", tool_call.id)
        self.assertEqual("get_weather", tool_call.name)
        self.assertEqual("sunny", tool_call.result)
        self.assertIsNotNone(second.usage)
        self.assertEqual(2, second.usage.completion_tokens)
        self.assertIsNotNone(result.total_usage)
        self.assertEqual(5, result.total_usage.reasoning_tokens)

    def test_message_timestamp_parsed(self):
        messages = AiConversationMessagesResult.from_json(self._sample_payload()).messages
        self.assertEqual(datetime(2026, 6, 1, 11, 59, 0), messages[0].timestamp)


@unittest.skipIf(
    os.environ.get("RAVENDB_LICENSE") is None and os.environ.get("RAVEN_License") is None,
    "Insufficient license permissions. Skipping on CI/CD.",
)
class TestGetConversationMessages(TestBase):
    def test_missing_conversation_returns_none(self):
        # A 404 for an unknown conversation must yield None, not an exception.
        result = self.store.ai.get_conversation_messages("conversations/does-not-exist")
        self.assertIsNone(result)

    def test_missing_conversation_returns_none_with_options(self):
        result = self.store.ai.get_conversation_messages(
            GetConversationMessagesOptions(conversation_id="conversations/does-not-exist")
        )
        self.assertIsNone(result)

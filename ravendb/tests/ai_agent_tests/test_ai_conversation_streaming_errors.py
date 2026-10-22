"""
Unit tests for errors written into a streamed conversation response (RavenDB-25401).

When the server fails after streaming already started (HTTP 200), it writes the standard error payload
as the final line of the stream. The client must raise it instead of parsing it as the conversation result.
No server is needed: the streamed response is faked.
"""

import json
import unittest

from ravendb.documents.operations.ai.agents.run_conversation_operation import RunConversationCommand
from ravendb.exceptions.raven_exceptions import AiException, RefusedToAnswerException

EXPECTED_REFUSAL = "I'm sorry, I can't help with that."


class _FakeStreamingResponse:
    def __init__(self, lines, status_code=200):
        self._lines = lines
        self.status_code = status_code

    def iter_lines(self, decode_unicode=False):
        return iter(self._lines)


def _streaming_command(chunks):
    return RunConversationCommand(
        agent_id="agents/1",
        conversation_id="conversations/1",
        stream_property_path="Answer",
        streamed_chunks_callback=chunks.append,
    )


class TestAiConversationStreamingErrors(unittest.TestCase):
    def test_error_payload_in_stream_raises_dispatched_exception(self):
        error_payload = {
            "Url": "/databases/db1/ai/agent",
            "Type": "Raven.Client.Exceptions.AI.AiException",
            "Message": "Failed to communicate with the agent",
            "Error": (
                "Raven.Client.Exceptions.AI.AiException: Failed to communicate with the agent"
                " ---> Raven.Client.Exceptions.AI.RefusedToAnswerException: "
                f"The model refused to answer: {EXPECTED_REFUSAL}"
            ),
            "RequestId": "req-refusal-123",
        }
        chunks = []
        command = _streaming_command(chunks)
        response = _FakeStreamingResponse([json.dumps("partial"), "", json.dumps(error_payload)])

        with self.assertRaises(AiException) as ctx:
            command.process_response(None, response, "http://localhost:8080/databases/db1/ai/agent")

        self.assertIs(AiException, type(ctx.exception))
        self.assertIn("RefusedToAnswerException", str(ctx.exception))
        self.assertIn(EXPECTED_REFUSAL, str(ctx.exception))
        self.assertEqual("req-refusal-123", ctx.exception.request_id)
        self.assertEqual(["partial"], chunks)
        self.assertIsNone(command.result)

    def test_refused_to_answer_payload_in_stream_raises_refused_to_answer_exception(self):
        error_payload = {
            "Type": "Raven.Client.Exceptions.AI.RefusedToAnswerException",
            "Message": EXPECTED_REFUSAL,
            "Error": f"Raven.Client.Exceptions.AI.RefusedToAnswerException: {EXPECTED_REFUSAL}",
            "Refusal": EXPECTED_REFUSAL,
            "FinishReason": "content_filter",
        }
        command = _streaming_command([])
        response = _FakeStreamingResponse([json.dumps(error_payload)])

        with self.assertRaises(RefusedToAnswerException) as ctx:
            command.process_response(None, response, "http://localhost:8080/databases/db1/ai/agent")

        self.assertEqual(EXPECTED_REFUSAL, ctx.exception.refusal)
        self.assertEqual("content_filter", ctx.exception.finish_reason)

    def test_final_result_in_stream_is_parsed(self):
        final = {
            "ConversationId": "conversations/1",
            "ChangeVector": "A:1",
            "Response": {"Answer": "streamed answer"},
            "ActionRequests": [],
        }
        chunks = []
        command = _streaming_command(chunks)
        response = _FakeStreamingResponse([json.dumps("streamed "), json.dumps("answer"), json.dumps(final)])

        command.process_response(None, response, "http://localhost:8080/databases/db1/ai/agent")

        self.assertEqual(["streamed ", "answer"], chunks)
        self.assertEqual("conversations/1", command.result.conversation_id)
        self.assertEqual({"Answer": "streamed answer"}, command.result.response)


if __name__ == "__main__":
    unittest.main()

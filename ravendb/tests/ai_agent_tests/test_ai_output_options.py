"""
Tests for the per-turn output schema override added in 7.2.6: AiOutputOptions, the
run_with_schema / stream_with_schema entry points, and how the options reach the wire.
"""

import json
import unittest

from ravendb.documents.ai.ai_conversation import AiConversation
from ravendb.documents.ai.ai_output_options import AiOutputOptions
from ravendb.documents.operations.ai.agents.run_conversation_operation import (
    ConversationRequestBody,
    ConversationResult,
    RunConversationOperation,
)
from ravendb.http.server_node import ServerNode


def _request_body(operation: RunConversationOperation) -> dict:
    request = operation.get_command(None).create_request(ServerNode("http://localhost:8080", "db"))
    return json.loads(request.data) if isinstance(request.data, str) else request.data


class TestAiOutputOptions(unittest.TestCase):
    def test_a_sample_object_is_sent_as_json_text(self):
        # The server reads SampleObject as a string, not as a nested object.
        options = AiOutputOptions(sample_object={"Name": "sample", "Age": 1})

        self.assertEqual({"Name": "sample", "Age": 1}, json.loads(options.to_json()["SampleObject"]))

    def test_a_sample_object_may_be_an_entity_with_to_json(self):
        class _Answer:
            def to_json(self):
                return {"Name": "sample"}

        self.assertEqual(
            {"Name": "sample"}, json.loads(AiOutputOptions(sample_object=_Answer()).to_json()["SampleObject"])
        )

    def test_an_explicit_schema_is_sent_verbatim(self):
        schema = '{"type":"object","properties":{"Name":{"type":"string"}}}'

        self.assertEqual(schema, AiOutputOptions(output_schema=schema).to_json()["OutputSchema"])

    def test_no_schema_asks_for_free_form_text(self):
        self.assertEqual({"NoSchema": True}, AiOutputOptions(no_schema=True).to_json())

    def test_empty_options_send_nothing(self):
        # Nothing set means the agent's own schema stays in charge.
        self.assertEqual({}, AiOutputOptions().to_json())

    def test_no_schema_cannot_be_combined_with_a_schema(self):
        with self.assertRaises(ValueError):
            AiOutputOptions(no_schema=True, output_schema="{}")
        with self.assertRaises(ValueError):
            AiOutputOptions(no_schema=True, sample_object={"Name": "x"})

    def test_an_empty_schema_string_is_rejected(self):
        with self.assertRaises(ValueError):
            AiOutputOptions(output_schema="")
        with self.assertRaises(ValueError):
            AiOutputOptions(output_schema="   ")

    def test_options_round_trip(self):
        for options in (
            AiOutputOptions(sample_object={"Name": "x"}),
            AiOutputOptions(output_schema='{"type":"object"}'),
            AiOutputOptions(no_schema=True),
        ):
            self.assertEqual(options.to_json(), AiOutputOptions.from_json(options.to_json()).to_json())

    def test_nothing_parses_to_nothing(self):
        self.assertIsNone(AiOutputOptions.from_json(None))
        self.assertIsNone(AiOutputOptions.from_json({}))


class TestOutputOptionsOnTheWire(unittest.TestCase):
    def test_the_request_body_carries_the_options(self):
        body = _request_body(
            RunConversationOperation(
                agent_id="agent",
                conversation_id="chats/1",
                prompt_parts=[],
                output_options=AiOutputOptions(no_schema=True),
            )
        )

        self.assertEqual({"NoSchema": True}, body["OutputOptions"])

    def test_a_turn_without_options_does_not_mention_them(self):
        # Leaving the key out is what keeps the agent's configured schema in charge.
        body = _request_body(RunConversationOperation(agent_id="agent", conversation_id="chats/1", prompt_parts=[]))

        self.assertNotIn("OutputOptions", body)

    def test_the_request_body_serializes_a_schema_override(self):
        body = ConversationRequestBody(output_options=AiOutputOptions(output_schema='{"type":"string"}')).to_json()

        self.assertEqual('{"type":"string"}', body["OutputOptions"]["OutputSchema"])

    def test_the_other_body_fields_are_untouched(self):
        body = _request_body(
            RunConversationOperation(
                agent_id="agent",
                conversation_id="chats/1",
                prompt_parts=[],
                output_options=AiOutputOptions(no_schema=True),
            )
        )

        for key in ("ActionResponses", "ArtificialActions", "CreationOptions", "UserPrompt"):
            self.assertIn(key, body)


class TestConversationRunEntryPoints(unittest.TestCase):
    def test_the_schema_overriding_entry_points_exist(self):
        for name in ("run", "run_with_schema", "stream", "stream_with_schema"):
            self.assertTrue(hasattr(AiConversation, name), name)

    def test_they_refuse_to_run_without_options(self):
        conversation = AiConversation.__new__(AiConversation)

        with self.assertRaises(ValueError):
            conversation.run_with_schema(None)
        with self.assertRaises(ValueError):
            conversation.stream_with_schema("Answer", lambda chunk: None, None)


class TestFreeFormAnswers(unittest.TestCase):
    def test_a_raw_text_answer_is_read_as_a_string(self):
        # With no schema the server answers with a bare string rather than an object.
        result = ConversationResult.from_json({"ConversationId": "chats/1", "Response": "just some prose"})

        self.assertEqual("just some prose", result.response)

    def test_a_structured_answer_is_still_read_as_an_object(self):
        result = ConversationResult.from_json({"ConversationId": "chats/1", "Response": {"Name": "sample"}})

        self.assertEqual({"Name": "sample"}, result.response)

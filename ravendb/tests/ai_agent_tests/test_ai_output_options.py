"""
Tests for the per-turn output schema override added in 7.2.6: AiOutputOptions, the
run_with_schema / stream_with_schema entry points, and how the options reach the wire.
"""

import json
import unittest
from unittest.mock import MagicMock

from ravendb.documents.ai.ai_answer import AiConversationStatus
from ravendb.documents.ai.ai_conversation import AiConversation
from ravendb.documents.ai.ai_output_options import AiOutputOptions
from ravendb.documents.operations.ai.agents.run_conversation_operation import (
    AiAgentActionRequest,
    ConversationRequestBody,
    ConversationResult,
    RunConversationOperation,
)
from ravendb.http.server_node import ServerNode


def _request(operation: RunConversationOperation):
    return operation.get_command(None).create_request(ServerNode("http://localhost:8080", "db"))


def _request_body(operation: RunConversationOperation) -> dict:
    request = _request(operation)
    return json.loads(request.data) if isinstance(request.data, str) else request.data


def _conversation(*results: ConversationResult):
    # The store only has to hand each operation to maintenance.send; the replies are canned.
    store = MagicMock()
    store.maintenance.send.side_effect = list(results)
    return AiConversation(store, agent_id="agents/1", conversation_id="chats/1"), store.maintenance.send


def _done(response) -> ConversationResult:
    return ConversationResult(conversation_id="chats/1", change_vector="A:1", response=response, action_requests=[])


def _sent_operations(send) -> list:
    return [call.args[0] for call in send.call_args_list]


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
        for name in ("run", "run_with_schema", "run_text", "stream", "stream_with_schema", "stream_text"):
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


class _AlternativeSchema:
    def __init__(self, summary: str = None, score: int = None):
        self.Summary = summary
        self.Score = score


class TestSchemaOverrideShorthands(unittest.TestCase):
    def test_a_string_is_taken_as_an_explicit_schema(self):
        schema = '{"name":"alt","strict":true,"schema":{"type":"object"}}'
        conversation, send = _conversation(_done({"Summary": "x", "Score": 1}))
        conversation.set_user_prompt("Rate yourself")

        conversation.run_with_schema(schema)

        self.assertEqual({"OutputSchema": schema}, _request_body(_sent_operations(send)[0])["OutputOptions"])

    def test_a_dict_is_taken_as_a_sample_object(self):
        conversation, send = _conversation(_done({"Summary": "x", "Score": 1}))
        conversation.set_user_prompt("Rate yourself")

        conversation.run_with_schema({"Summary": "a short summary", "Score": 5})

        options = _request_body(_sent_operations(send)[0])["OutputOptions"]
        self.assertEqual({"Summary": "a short summary", "Score": 5}, json.loads(options["SampleObject"]))

    def test_an_entity_is_taken_as_a_sample_object(self):
        conversation, send = _conversation(_done({"Summary": "x", "Score": 1}))
        conversation.set_user_prompt("Rate yourself")

        answer = conversation.run_with_schema(_AlternativeSchema("a short summary", 5))

        options = _request_body(_sent_operations(send)[0])["OutputOptions"]
        self.assertEqual({"Summary": "a short summary", "Score": 5}, json.loads(options["SampleObject"]))
        self.assertEqual(AiConversationStatus.DONE, answer.status)

    def test_an_empty_schema_string_is_rejected(self):
        conversation, send = _conversation()
        conversation.set_user_prompt("Rate yourself")

        with self.assertRaises(ValueError):
            conversation.run_with_schema("  ")
        send.assert_not_called()

    def test_stream_with_schema_takes_a_sample_object(self):
        conversation, send = _conversation(_done({"Summary": "x", "Score": 1}))
        conversation.set_user_prompt("Rate yourself")

        conversation.stream_with_schema("Summary", lambda chunk: None, _AlternativeSchema("a short summary", 5))

        operation = _sent_operations(send)[0]
        self.assertIn("streamPropertyPath=Summary", _request(operation).url)
        self.assertIn("SampleObject", _request_body(operation)["OutputOptions"])


class TestRawTextShorthands(unittest.TestCase):
    def test_run_text_asks_for_no_schema_and_returns_the_text(self):
        conversation, send = _conversation(_done("Hello! I am a helpful assistant."))
        conversation.set_user_prompt("Say hello")

        answer = conversation.run_text()

        self.assertEqual("Hello! I am a helpful assistant.", answer.answer)
        self.assertEqual({"NoSchema": True}, _request_body(_sent_operations(send)[0])["OutputOptions"])

    def test_stream_text_streams_the_whole_answer_without_a_schema(self):
        conversation, send = _conversation(_done("Hello"))
        conversation.set_user_prompt("Say hello")

        conversation.stream_text(lambda chunk: None)

        operation = _sent_operations(send)[0]
        self.assertIn("streaming=true&streamPropertyPath=&", _request(operation).url)
        self.assertEqual({"NoSchema": True}, _request_body(operation)["OutputOptions"])

    def test_stream_text_needs_a_callback(self):
        conversation, _ = _conversation()

        with self.assertRaises(ValueError):
            conversation.stream_text(None)

    def test_no_schema_streaming_does_not_need_a_property_path(self):
        conversation, send = _conversation(_done("Hello"))
        conversation.set_user_prompt("Say hello")

        conversation.stream_with_schema(None, lambda chunk: None, AiOutputOptions(no_schema=True))

        self.assertIn("streaming=true", _request(_sent_operations(send)[0]).url)

    def test_a_turn_without_streaming_does_not_ask_for_it(self):
        operation = RunConversationOperation(agent_id="agent", conversation_id="chats/1", prompt_parts=[])

        self.assertNotIn("streaming", _request(operation).url)


class TestNoArgsTools(unittest.TestCase):
    @staticmethod
    def _tool_call(arguments: str) -> ConversationResult:
        return ConversationResult(
            conversation_id="chats/1",
            change_vector="A:1",
            response=None,
            action_requests=[AiAgentActionRequest(name="get_current_time", tool_id="call-1", arguments=arguments)],
        )

    def test_handle_no_args_calls_the_handler_without_arguments(self):
        conversation, send = _conversation(self._tool_call(""), _done({"Answer": "noon"}))
        conversation.handle_no_args("get_current_time", lambda: "2026-01-01T12:00:00Z")
        conversation.set_user_prompt("What time is it?")

        answer = conversation.run()

        self.assertEqual(AiConversationStatus.DONE, answer.status)
        responses = _request_body(_sent_operations(send)[1])["ActionResponses"]
        self.assertEqual([{"ToolId": "call-1", "Content": "2026-01-01T12:00:00Z"}], responses)

    def test_receive_no_args_gets_the_request(self):
        received = []
        conversation, send = _conversation(self._tool_call("{}"), _done({"Answer": "pong"}))

        def on_ping(request):
            received.append(request)
            conversation.add_action_response(request.tool_id, "pong")

        conversation.receive_no_args("get_current_time", on_ping)
        conversation.set_user_prompt("Ping")

        conversation.run()

        self.assertEqual(["call-1"], [request.tool_id for request in received])
        self.assertEqual("pong", _request_body(_sent_operations(send)[1])["ActionResponses"][0]["Content"])

    def test_a_failing_no_args_handler_reports_the_error_to_the_model(self):
        def broken():
            raise RuntimeError("clock unavailable")

        conversation, send = _conversation(self._tool_call(""), _done({"Answer": "unknown"}))
        conversation.handle_no_args("get_current_time", broken)
        conversation.set_user_prompt("What time is it?")

        conversation.run()

        content = _request_body(_sent_operations(send)[1])["ActionResponses"][0]["Content"]
        self.assertIn("clock unavailable", content)

    def test_a_no_args_tool_name_can_be_registered_only_once(self):
        conversation, _ = _conversation()
        conversation.handle_no_args("get_current_time", lambda: "now")

        with self.assertRaises(ValueError):
            conversation.receive_no_args("get_current_time", lambda request: None)

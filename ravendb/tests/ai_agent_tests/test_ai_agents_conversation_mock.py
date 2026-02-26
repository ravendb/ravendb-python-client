"""
Hybrid tests for AI agent conversation flow.

Agent CRUD (create/delete) runs against the real embedded server.
The actual conversation call (RunConversationOperation / maintenance.send) is
mocked so no LLM API key is required.
"""

import json
import os
import unittest
from unittest.mock import patch

from ravendb import (
    AiAgentConfiguration,
    AiAgentToolAction,
    AiAgentToolQuery,
    AiConversationCreationOptions,
)
from ravendb.documents.ai.ai_answer import AiConversationStatus
from ravendb.documents.ai.ai_conversation import AiHandleErrorStrategy, UnhandledActionEventArgs
from ravendb.documents.operations.ai import AiConnectionString, AiModelType
from ravendb.documents.operations.ai.agents import (
    AiAgentActionRequest,
    AiUsage,
    ConversationResult,
    DeleteAiAgentOperation,
    RunConversationOperation,
)
from ravendb.documents.operations.ai.open_ai_settings import OpenAiSettings
from ravendb.documents.operations.connection_string.put_connection_string_operation import PutConnectionStringOperation
from ravendb.tests.test_base import TestBase

CONNECTION_STRING_NAME = "conv-mock-cs"
AGENT_ID = "conv-mock-agent"


def _make_usage(prompt=10, completion=20):
    return AiUsage(prompt_tokens=prompt, completion_tokens=completion, total_tokens=prompt + completion)


def _done_result(response=None, conversation_id="conversations/1", change_vector="A:1"):
    return ConversationResult(
        conversation_id=conversation_id,
        change_vector=change_vector,
        response=response or {"answer": "42"},
        usage=_make_usage(),
        action_requests=[],
    )


def _action_result(action_name, tool_id, arguments, conversation_id="conversations/1", change_vector="A:1"):
    return ConversationResult(
        conversation_id=conversation_id,
        change_vector=change_vector,
        response=None,
        usage=_make_usage(),
        action_requests=[AiAgentActionRequest(name=action_name, tool_id=tool_id, arguments=json.dumps(arguments))],
    )


@unittest.skipIf(os.environ.get("RAVENDB_LICENSE") is None, "Insufficient license permissions. Skipping on CI/CD.")
class TestAiAgentConversationMock(TestBase):
    """
    Hybrid tests: agent CRUD uses the real server; conversation calls are mocked.
    The mock target is store.maintenance.send - only RunConversationOperation
    calls are intercepted; everything else is forwarded to the real executor.
    """

    def setUp(self):
        super().setUp()
        cs = AiConnectionString(
            name=CONNECTION_STRING_NAME,
            identifier=CONNECTION_STRING_NAME,
            model_type=AiModelType.CHAT,
            openai_settings=OpenAiSettings(
                api_key="dummy-key",
                endpoint="https://api.openai.com/v1",
                model="gpt-4",
            ),
        )
        self.store.maintenance.send(PutConnectionStringOperation(cs))
        agent = AiAgentConfiguration(
            identifier=AGENT_ID,
            name="ConvMockAgent",
            connection_string_name=CONNECTION_STRING_NAME,
            system_prompt="You are a helpful assistant.",
            sample_object='{"answer": "embed your answer here"}',
            queries=[
                AiAgentToolQuery(
                    name="get-orders",
                    description="Retrieve orders.",
                    query="from Orders",
                    parameters_sample_object="{}",
                ),
            ],
            actions=[
                AiAgentToolAction(
                    name="store-result",
                    description="Store the result.",
                    parameters_sample_object='{"result": "embed result here"}',
                ),
            ],
        )
        self.store.ai.add_or_update_agent(agent)
        self._real_send = self.store.maintenance.send

    def tearDown(self):
        try:
            self.store.maintenance.send(DeleteAiAgentOperation(AGENT_ID))
        except Exception:
            pass
        super().tearDown()

    def _patched_send(self, mock_conv_result_fn):
        real_send = self._real_send

        def _send(operation):
            if isinstance(operation, RunConversationOperation):
                return mock_conv_result_fn()
            return real_send(operation)

        return _send

    def test_basic_conversation_returns_answer(self):
        with patch.object(
            self.store.maintenance,
            "send",
            side_effect=self._patched_send(lambda: _done_result(response={"answer": "Hello!"})),
        ):
            chat = self.store.ai.conversation(AGENT_ID, "conversations/")
            chat.set_user_prompt("Hi there")
            result = chat.run()
        self.assertEqual(AiConversationStatus.DONE, result.status)
        self.assertEqual("Hello!", result.answer["answer"])

    def test_conversation_id_and_change_vector_are_stored(self):
        with patch.object(
            self.store.maintenance,
            "send",
            side_effect=self._patched_send(
                lambda: _done_result(conversation_id="conversations/99", change_vector="A:99")
            ),
        ):
            chat = self.store.ai.conversation(AGENT_ID, "conversations/")
            chat.set_user_prompt("Hello")
            chat.run()
        self.assertEqual("conversations/99", chat._conversation_id)
        self.assertEqual("A:99", chat._change_vector)

    def test_usage_is_populated(self):
        with patch.object(self.store.maintenance, "send", side_effect=self._patched_send(lambda: _done_result())):
            chat = self.store.ai.conversation(AGENT_ID, "conversations/")
            chat.set_user_prompt("Hello")
            result = chat.run()
        self.assertIsNotNone(result.usage)
        self.assertEqual(10, result.usage.prompt_tokens)
        self.assertEqual(20, result.usage.completion_tokens)
        self.assertEqual(30, result.usage.total_tokens)

    def test_elapsed_is_populated(self):
        with patch.object(self.store.maintenance, "send", side_effect=self._patched_send(lambda: _done_result())):
            chat = self.store.ai.conversation(AGENT_ID, "conversations/")
            chat.set_user_prompt("Hello")
            result = chat.run()
        self.assertIsNotNone(result.elapsed)

    def test_context_manager_usage(self):
        with patch.object(
            self.store.maintenance,
            "send",
            side_effect=self._patched_send(lambda: _done_result(response={"answer": "ctx"})),
        ):
            with self.store.ai.conversation(AGENT_ID, "conversations/") as chat:
                chat.set_user_prompt("Hello from context manager")
                result = chat.run()
        self.assertEqual(AiConversationStatus.DONE, result.status)
        self.assertEqual("ctx", result.answer["answer"])

    # ------------------------------------------------------------------
    # Action handler flow (handle / receive)
    # ------------------------------------------------------------------

    def test_handle_invokes_handler_and_sends_response(self):
        calls = []
        responses = iter(
            [
                _action_result("store-result", "tool-1", {"result": "data"}),
                _done_result(response={"answer": "stored"}),
            ]
        )

        with patch.object(self.store.maintenance, "send", side_effect=self._patched_send(lambda: next(responses))):
            chat = self.store.ai.conversation(AGENT_ID, "conversations/")
            chat.set_user_prompt("Store something")
            chat.handle(
                "store-result", lambda args: calls.append(args) or "ok", AiHandleErrorStrategy.SEND_ERRORS_TO_MODEL
            )
            result = chat.run()

        self.assertEqual(AiConversationStatus.DONE, result.status)
        self.assertEqual(1, len(calls))
        self.assertEqual({"result": "data"}, calls[0])

    def test_receive_invokes_handler_with_request_and_args(self):
        received = []
        responses = iter(
            [
                _action_result("store-result", "tool-2", {"result": "payload"}),
                _done_result(),
            ]
        )

        def my_receiver(request, args):
            received.append((request, args))
            chat.add_action_response(request.tool_id, "done")

        with patch.object(self.store.maintenance, "send", side_effect=self._patched_send(lambda: next(responses))):
            chat = self.store.ai.conversation(AGENT_ID, "conversations/")
            chat.set_user_prompt("Do something")
            chat.receive("store-result", my_receiver, AiHandleErrorStrategy.SEND_ERRORS_TO_MODEL)
            result = chat.run()

        self.assertEqual(AiConversationStatus.DONE, result.status)
        self.assertEqual(1, len(received))
        req, args = received[0]
        self.assertIsInstance(req, AiAgentActionRequest)
        self.assertEqual("tool-2", req.tool_id)
        self.assertEqual({"result": "payload"}, args)

    def test_multi_turn_action_loop(self):
        responses = iter(
            [
                _action_result("store-result", "tool-a", {"result": "first"}),
                _action_result("store-result", "tool-b", {"result": "second"}),
                _done_result(response={"answer": "all done"}),
            ]
        )

        with patch.object(self.store.maintenance, "send", side_effect=self._patched_send(lambda: next(responses))):
            chat = self.store.ai.conversation(AGENT_ID, "conversations/")
            chat.set_user_prompt("Do two things")
            chat.handle("store-result", lambda args: "handled", AiHandleErrorStrategy.SEND_ERRORS_TO_MODEL)
            result = chat.run()

        self.assertEqual(AiConversationStatus.DONE, result.status)

    # ------------------------------------------------------------------
    # Error handling strategies
    # ------------------------------------------------------------------

    def test_handler_error_send_to_model(self):
        responses = iter(
            [
                _action_result("store-result", "tool-err", {"result": "x"}),
                _done_result(response={"answer": "recovered"}),
            ]
        )

        def bad_handler(args):
            raise ValueError("something went wrong")

        with patch.object(self.store.maintenance, "send", side_effect=self._patched_send(lambda: next(responses))):
            chat = self.store.ai.conversation(AGENT_ID, "conversations/")
            chat.set_user_prompt("Trigger error")
            chat.handle("store-result", bad_handler, AiHandleErrorStrategy.SEND_ERRORS_TO_MODEL)
            result = chat.run()

        # Error was sent to model as a response; conversation continued and finished
        self.assertEqual(AiConversationStatus.DONE, result.status)

    def test_handler_error_raise_immediately(self):
        def mock_send():
            return _action_result("store-result", "tool-err2", {"result": "x"})

        def bad_handler(args):
            raise RuntimeError("fatal error")

        with patch.object(self.store.maintenance, "send", side_effect=self._patched_send(mock_send)):
            chat = self.store.ai.conversation(AGENT_ID, "conversations/")
            chat.set_user_prompt("Trigger fatal error")
            chat.handle("store-result", bad_handler, AiHandleErrorStrategy.RAISE_IMMEDIATELY)
            with self.assertRaises(RuntimeError):
                chat.run()

    # ------------------------------------------------------------------
    # Unhandled action event
    # ------------------------------------------------------------------

    def test_on_unhandled_action_is_called(self):
        unhandled = []
        responses = iter(
            [
                _action_result("unknown-action", "t-99", {"x": 1}),
                _done_result(),
            ]
        )

        def on_unhandled(event_args):
            unhandled.append(event_args)
            event_args.sender.add_action_response(event_args.action.tool_id, "fallback")

        with patch.object(self.store.maintenance, "send", side_effect=self._patched_send(lambda: next(responses))):
            chat = self.store.ai.conversation(AGENT_ID, "conversations/")
            chat.on_unhandled_action = on_unhandled
            chat.set_user_prompt("Do something unhandled")
            result = chat.run()

        self.assertEqual(AiConversationStatus.DONE, result.status)
        self.assertEqual(1, len(unhandled))
        self.assertIsInstance(unhandled[0], UnhandledActionEventArgs)
        self.assertEqual("unknown-action", unhandled[0].action.name)

    def test_no_handler_raises_runtime_error(self):
        responses = iter(
            [
                _action_result("missing-action", "t-0", {}),
            ]
        )

        with patch.object(self.store.maintenance, "send", side_effect=self._patched_send(lambda: next(responses))):
            chat = self.store.ai.conversation(AGENT_ID, "conversations/")
            chat.set_user_prompt("Trigger missing handler")
            with self.assertRaises(RuntimeError):
                chat.run()

    # ------------------------------------------------------------------
    # Artificial action injection
    # ------------------------------------------------------------------

    def test_add_artificial_action_with_response(self):
        # AiConversation clears _artificial_actions in the finally block after sending,
        # so we snapshot the list contents at call time before the clear happens.
        captured_artificial_actions = []

        def capturing_send(operation):
            if isinstance(operation, RunConversationOperation):
                # snapshot before the finally-block clear
                captured_artificial_actions.extend(list(operation._artificial_actions))
                return _done_result(response={"answer": "injected"})
            return self._real_send(operation)

        with patch.object(self.store.maintenance, "send", side_effect=capturing_send):
            chat = self.store.ai.conversation(AGENT_ID, "conversations/")
            chat.set_user_prompt("Use injected context")
            chat.add_artificial_action_with_response("get-orders", {"orders": []})
            result = chat.run()

        self.assertEqual(AiConversationStatus.DONE, result.status)
        self.assertEqual(1, len(captured_artificial_actions))
        self.assertEqual("get-orders", captured_artificial_actions[0].tool_id)

    def test_add_artificial_action_validates_tool_id(self):
        chat = self.store.ai.conversation(AGENT_ID, "conversations/")
        with self.assertRaises(ValueError):
            chat.add_artificial_action_with_response("", "some response")
        with self.assertRaises(ValueError):
            chat.add_artificial_action_with_response("   ", "some response")

    def test_add_artificial_action_validates_response_not_none(self):
        chat = self.store.ai.conversation(AGENT_ID, "conversations/")
        with self.assertRaises(ValueError):
            chat.add_artificial_action_with_response("get-orders", None)

    # ------------------------------------------------------------------
    # Conversation with creation options (parameters / expiration)
    # ------------------------------------------------------------------

    def test_conversation_with_creation_options(self):
        options = AiConversationCreationOptions(
            parameters={"customerId": "C-42"},
            expiration_in_sec=3600,
        )
        sent_operations = []

        def capturing_send(operation):
            sent_operations.append(operation)
            return self._patched_send(lambda: _done_result(response={"answer": "ok"}))(operation)

        with patch.object(self.store.maintenance, "send", side_effect=capturing_send):
            chat = self.store.ai.conversation(AGENT_ID, "conversations/", creation_options=options)
            chat.set_user_prompt("Hello with options")
            result = chat.run()

        self.assertEqual(AiConversationStatus.DONE, result.status)
        conv_op = next(o for o in sent_operations if isinstance(o, RunConversationOperation))
        self.assertEqual({"customerId": "C-42"}, conv_op._options.parameters)
        self.assertEqual(3600, conv_op._options.expiration_in_sec)

    # ------------------------------------------------------------------
    # Streaming
    # ------------------------------------------------------------------

    def test_stream_collects_chunks_and_returns_answer(self):
        chunks = []

        def mock_send():
            return _done_result(response={"answer": "streamed answer"})

        with patch.object(self.store.maintenance, "send", side_effect=self._patched_send(mock_send)):
            chat = self.store.ai.conversation(AGENT_ID, "conversations/")
            chat.set_user_prompt("Stream this")
            result = chat.stream(stream_property_path="answer", on_chunk=chunks.append)

        self.assertEqual(AiConversationStatus.DONE, result.status)

    # ------------------------------------------------------------------
    # set_user_prompt validation
    # ------------------------------------------------------------------

    def test_set_user_prompt_empty_raises(self):
        chat = self.store.ai.conversation(AGENT_ID, "conversations/")
        with self.assertRaises(ValueError):
            chat.set_user_prompt("")
        with self.assertRaises(ValueError):
            chat.set_user_prompt("   ")

    def test_required_actions_before_run_raises(self):
        chat = self.store.ai.conversation(AGENT_ID, "conversations/")
        with self.assertRaises(RuntimeError):
            _ = chat.required_actions

    def test_required_actions_after_run_returns_list(self):
        with patch.object(self.store.maintenance, "send", side_effect=self._patched_send(lambda: _done_result())):
            chat = self.store.ai.conversation(AGENT_ID, "conversations/")
            chat.set_user_prompt("Hello")
            chat.run()
        self.assertIsInstance(chat.required_actions, list)
        self.assertEqual(0, len(chat.required_actions))

    # ------------------------------------------------------------------
    # Duplicate action handler registration
    # ------------------------------------------------------------------

    def test_duplicate_action_handler_raises(self):
        chat = self.store.ai.conversation(AGENT_ID, "conversations/")
        chat.handle("store-result", lambda args: None, AiHandleErrorStrategy.SEND_ERRORS_TO_MODEL)
        with self.assertRaises(ValueError):
            chat.handle("store-result", lambda args: None, AiHandleErrorStrategy.SEND_ERRORS_TO_MODEL)


if __name__ == "__main__":
    unittest.main()

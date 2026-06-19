import json
import unittest
from types import SimpleNamespace

from ravendb.documents.ai.ai_conversation import AiConversation, AiHandleErrorStrategy
from ravendb.documents.ai.ai_operations import AiOperations
from ravendb.documents.operations.ai.agents import (
    AiAgentActionRequest,
    AiUsage,
    ConversationResult,
    RunConversationOperation,
)
from ravendb.documents.operations.ai.agents.run_conversation_operation import RunConversationCommand
from ravendb.http.server_node import ServerNode


def _usage():
    return AiUsage(prompt_tokens=10, completion_tokens=20, total_tokens=30)


def _done_result():
    return ConversationResult(
        conversation_id="conversations/1",
        change_vector="A:1",
        response={"answer": "ok"},
        usage=_usage(),
        action_requests=[],
    )


def _action_result(name, tool_id, arguments=None):
    return ConversationResult(
        conversation_id="conversations/1",
        change_vector="A:1",
        response=None,
        usage=_usage(),
        action_requests=[AiAgentActionRequest(name=name, tool_id=tool_id, arguments=json.dumps(arguments or {}))],
    )


class _FakeMaintenance:
    def __init__(self, results):
        self._results = list(results)
        self.sent_operations = []

    def send(self, operation):
        self.sent_operations.append(operation)
        return self._results.pop(0)


class _FakeStore:
    def __init__(self, results):
        self.maintenance = _FakeMaintenance(results)


class TestAiConversationDebug(unittest.TestCase):
    def _url(self, debug):
        kwargs = {} if debug is None else {"debug": debug}
        command = RunConversationCommand(agent_id="agents/1", conversation_id="conversations/1", **kwargs)
        return command.create_request(ServerNode("http://localhost:8080", "db1")).url

    def test_debug_true_appended_to_url(self):
        self.assertIn("&debug=True", self._url(True))

    def test_debug_false_appended_to_url(self):
        self.assertIn("&debug=False", self._url(False))

    def test_debug_none_omits_param(self):
        self.assertNotIn("debug", self._url(None))

    def test_ai_operations_conversation_threads_debug(self):
        ops = AiOperations(SimpleNamespace())
        conversation = ops.conversation("agents/1", "conversations/", debug=True)
        self.assertTrue(conversation._debug)

    def test_run_passes_debug_to_operation(self):
        store = _FakeStore([_done_result()])
        conversation = AiConversation(store, agent_id="agents/1", debug=True)
        conversation.set_user_prompt("hi")
        conversation.run()

        sent = store.maintenance.sent_operations
        self.assertEqual(1, len(sent))
        self.assertIsInstance(sent[0], RunConversationOperation)
        self.assertTrue(sent[0]._debug)

    def test_already_dispatched_tool_is_not_invoked_twice(self):
        # Server returns the same tool-id across two turns; the handler must run only once.
        store = _FakeStore([_action_result("act", "t1"), _action_result("act", "t1")])
        conversation = AiConversation(store, agent_id="agents/1")

        calls = []
        conversation.handle(
            "act", lambda args: calls.append(args) or {"ok": True}, AiHandleErrorStrategy.RAISE_IMMEDIATELY
        )

        conversation.set_user_prompt("go")
        conversation.run()  # must terminate, not loop forever

        self.assertEqual(1, len(calls))
        self.assertEqual(2, len(store.maintenance.sent_operations))


if __name__ == "__main__":
    unittest.main()

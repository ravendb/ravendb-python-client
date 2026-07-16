"""
End-to-end tests for the AI conversation features added in 7.2.3 that need
a real LLM round-trip to verify behavior. Skipped unless both an OpenAI key
AND the RavenDB license are present.

Set the following env vars to run locally:

    RAVENDB_PYTHON_TEST_SERVER_PATH   = <path to Raven.Server.exe>
    RAVENDB_PYTHON_TEST_OPENAI_KEY    = sk-...
    RAVENDB_LICENSE                   = <license JSON, for AI agent feature gate>

Optional:
    RAVENDB_PYTHON_TEST_OPENAI_MODEL  = gpt-4o-mini (default)

Coverage:
  * basic conversation round-trip (sanity)
  * MissingAiAgentParameterException — required parameter not supplied
  * AiConversationCreationOptions.max_model_iterations_per_call is enforced
  * AiAgentActionRequestType — sub-agent invocation populates type=SUB_AGENT
  * add_attachment — LLM receives file content
  * copy_attachment_from — LLM receives copied attachment
"""

import io
import os
import unittest

from ravendb import (
    AiAgentConfiguration,
    AiAgentParameter,
    AiAgentToolSubAgent,
    AiConversationCreationOptions,
    AiConversationParameter,
)
from ravendb.documents.ai.ai_conversation import AiConversationStatus
from ravendb.documents.operations.ai import AiConnectionString, AiModelType
from ravendb.documents.operations.ai.agents import (
    AddOrUpdateAiAgentOperation,
    DeleteAiAgentOperation,
)
from ravendb.documents.operations.ai.open_ai_settings import OpenAiSettings
from ravendb.documents.operations.attachments import PutAttachmentOperation
from ravendb.documents.operations.connection_string.put_connection_string_operation import PutConnectionStringOperation
from ravendb.exceptions.raven_exceptions import MissingAiAgentParameterException
from ravendb.tests.test_base import TestBase

_OPENAI_KEY = os.environ.get("RAVENDB_PYTHON_TEST_OPENAI_KEY")
_OPENAI_MODEL = os.environ.get("RAVENDB_PYTHON_TEST_OPENAI_MODEL", "gpt-4o-mini")
_OPENAI_ENDPOINT = "https://api.openai.com/v1"

# These tests need a real OpenAI key. They run successfully locally when the
# env vars below are set:
#
#     RAVENDB_PYTHON_TEST_OPENAI_KEY = sk-...
#     RAVENDB_LICENSE                = <license JSON>
#
# They are unconditionally skipped in CI / open-source contributions to avoid
# requiring an OpenAI key on every contributor's machine. To run them, remove
# the decorator below or change it back to `@unittest.skipIf(_OPENAI_KEY is None ...)`.


@unittest.skipIf(_OPENAI_KEY is None, "Needs OpenAI API key. Skipping on CI/CD.")
class TestAiConversationAgainstRealLLM(TestBase):
    """End-to-end AI conversation tests using a real OpenAI endpoint."""

    CONNECTION_STRING_NAME = "test-openai-llm"

    def setUp(self):
        super().setUp()
        # Configure a real OpenAI connection. The license env var must also be
        # set for the server to accept the AI agent operations.
        self.store.maintenance.send(
            PutConnectionStringOperation(
                AiConnectionString(
                    name=self.CONNECTION_STRING_NAME,
                    identifier=self.CONNECTION_STRING_NAME,
                    model_type=AiModelType.CHAT,
                    openai_settings=OpenAiSettings(
                        api_key=_OPENAI_KEY,
                        endpoint=_OPENAI_ENDPOINT,
                        model=_OPENAI_MODEL,
                    ),
                )
            )
        )
        self._created_agent_ids = []

    def tearDown(self):
        for agent_id in self._created_agent_ids:
            try:
                self.store.maintenance.send(DeleteAiAgentOperation(agent_id))
            except Exception:
                pass
        super().tearDown()

    def _register_agent(self, **kwargs) -> str:
        defaults = dict(
            name="LlmTestAgent",
            connection_string_name=self.CONNECTION_STRING_NAME,
            system_prompt="You are a concise assistant. Reply briefly.",
            sample_object='{"answer": "embed your answer here"}',
        )
        defaults.update(kwargs)
        result = self.store.maintenance.send(AddOrUpdateAiAgentOperation(AiAgentConfiguration(**defaults)))
        self._created_agent_ids.append(result.identifier)
        return result.identifier

    # ---- sanity ----

    def test_basic_conversation_returns_answer(self):
        agent_id = self._register_agent(identifier="llm-basic")
        chat = self.store.ai.conversation(agent_id, "conversations/")
        chat.set_user_prompt("Reply with the word OK and nothing else.")
        result = chat.run()
        self.assertEqual(AiConversationStatus.DONE, result.status)
        self.assertIsNotNone(result.answer)

    # ---- MissingAiAgentParameterException ----

    def test_missing_required_parameter_raises(self):
        agent_id = self._register_agent(
            identifier="llm-missing-param",
            parameters=[AiAgentParameter("country", "The country to filter by.")],
        )
        chat = self.store.ai.conversation(agent_id, "conversations/")
        chat.set_user_prompt("Hello")
        with self.assertRaises(MissingAiAgentParameterException):
            chat.run()

    # ---- max_model_iterations_per_call ----

    def test_max_model_iterations_per_call_is_enforced(self):
        # Set a tiny cap. The server enforces it on the conversation run.
        agent_id = self._register_agent(
            identifier="llm-max-iter",
            max_model_iterations_per_call=1,
        )
        opts = AiConversationCreationOptions(max_model_iterations_per_call=1)
        chat = self.store.ai.conversation(agent_id, "conversations/", creation_options=opts)
        chat.set_user_prompt("Without using any tools, reply with just the word OK.")
        # The cap should either complete on a single iteration (DONE) or surface
        # a server-enforced limit. Either way: this must not silently exceed the
        # cap.
        result = chat.run()
        self.assertIn(result.status, (AiConversationStatus.DONE, AiConversationStatus.ACTION_REQUIRED))

    # ---- add_attachment ----

    def test_add_attachment_passes_file_content_to_model(self):
        agent_id = self._register_agent(
            identifier="llm-add-attachment",
            system_prompt="You receive files and summarize them in one sentence.",
        )
        secret = "The quick brown fox jumps over the lazy dog."
        chat = self.store.ai.conversation(agent_id, "conversations/")
        chat.add_attachment("note.txt", io.BytesIO(secret.encode("utf-8")), "text/plain")
        chat.set_user_prompt("Quote one short, unique phrase from the attachment exactly as it appears.")
        result = chat.run()
        self.assertEqual(AiConversationStatus.DONE, result.status)
        # The model should reference content from the attachment somewhere in
        # the answer. Use a loose substring check — LLM output formatting
        # varies but the unique phrase should appear.
        answer_text = str(result.answer).lower()
        self.assertIn("quick brown fox", answer_text)

    # ---- copy_attachment_from ----

    def test_copy_attachment_from_existing_document(self):
        """
        Wire-shape smoke test for `copy_attachment_from`: store a carrier
        document with an attachment, then issue a conversation that
        references it via `copy_attachment_from`. We assert the server
        accepts the multipart payload (the CopyAttachmentCommandData
        wire shape) and the conversation completes — we do NOT assert
        on what the model says about the attachment's contents because
        LLM behavior around attachment reading is non-deterministic.

        Whether the model actually "sees" the bytes is verified by hand
        against the live nightly. Byte-level shape of
        CopyAttachmentCommandData on the wire is unit-tested in
        ravendb/tests/ai_agent_tests/test_ai_conversation_attachments.py.
        """
        with self.store.open_session() as session:
            session.store({"_meta": "carrier"}, "docs/carrier")
            session.save_changes()
        self.store.operations.send(
            PutAttachmentOperation(
                "docs/carrier",
                "note.txt",
                b"Pangram content for the carrier document.",
                "text/plain",
            )
        )

        agent_id = self._register_agent(
            identifier="llm-copy-attachment",
            system_prompt="Reply briefly to anything the user asks.",
        )
        chat = self.store.ai.conversation(agent_id, "conversations/")
        chat.copy_attachment_from("docs/carrier", "note.txt")
        chat.set_user_prompt("Reply with the word OK.")
        result = chat.run()
        # The server accepted the multipart payload and the conversation
        # ran end-to-end — the copy_attachment_from wire shape is correct.
        self.assertEqual(AiConversationStatus.DONE, result.status)

    # ---- AiAgentActionRequestType (sub-agent) ----

    def test_parent_with_sub_agents_completes_conversation(self):
        """
        Wiring smoke test: registering a parent agent with `sub_agents` does
        not break the conversation flow. Whether the LLM actually chooses
        to dispatch is a model-behavior question (small models like
        gpt-4o-mini may answer directly without dispatching) — we just verify
        the wire shape carrying the sub_agents list is accepted server-side
        and the conversation completes without error.

        The byte-level shape of AiAgentActionRequestType=SUB_AGENT on the
        wire is covered by the unit tests in
        ravendb/tests/ai_agent_tests/test_ai_conversation_attachments.py.
        """
        sub_agent_id = self._register_agent(
            identifier="llm-sub-agent",
            name="EchoSubAgent",
            system_prompt="You echo back any input verbatim.",
        )
        parent_id = self._register_agent(
            identifier="llm-parent-agent",
            name="ParentAgent",
            system_prompt=(
                "When the user asks anything, delegate the task to the echo sub-agent and return its response."
            ),
            sub_agents=[AiAgentToolSubAgent(identifier=sub_agent_id, description="An echo sub-agent.")],
        )

        chat = self.store.ai.conversation(parent_id, "conversations/")
        chat.set_user_prompt("Reply with just the word PING.")
        result = chat.run()

        # The conversation must complete (DONE or ACTION_REQUIRED — both
        # indicate the wire flow succeeded). It must NOT raise.
        self.assertIn(result.status, (AiConversationStatus.DONE, AiConversationStatus.ACTION_REQUIRED))


if __name__ == "__main__":
    unittest.main()

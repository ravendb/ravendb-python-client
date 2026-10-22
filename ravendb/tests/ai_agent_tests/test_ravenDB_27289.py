import os
import unittest

from ravendb import AiAgentConfiguration, AiAgentParameter, AiAgentToolQuery, AiConversationCreationOptions
from ravendb.documents.operations.ai import AiConnectionString, AiModelType
from ravendb.documents.operations.ai.agents import AddOrUpdateAiAgentOperation, DeleteAiAgentOperation
from ravendb.documents.operations.ai.open_ai_settings import OpenAiSettings
from ravendb.documents.operations.connection_string.put_connection_string_operation import PutConnectionStringOperation
from ravendb.exceptions.raven_exceptions import (
    AiAgentParameterCollisionException,
    RavenException,
    UnsupportedAiQueryParameterException,
)
from ravendb.tests.test_base import TestBase

_OPTIONAL_SCOPING_PARAMETER = (
    '{"type":"object","properties":{"TelegramUsername":{"type":"string"}},"required":[],"additionalProperties":false}'
)


@unittest.skipIf(os.environ.get("RAVENDB_LICENSE") is None, "Insufficient license permissions. Skipping on CI/CD.")
class TestRavenDB27289(TestBase):
    CONNECTION_STRING_NAME = "fake-connection"
    AGENT_IDENTIFIER = "support-assistant"

    def setUp(self):
        super().setUp()
        # The refusals happen before the provider is contacted, so nothing has to answer at this address.
        self.store.maintenance.send(
            PutConnectionStringOperation(
                AiConnectionString(
                    name=self.CONNECTION_STRING_NAME,
                    identifier=self.CONNECTION_STRING_NAME,
                    model_type=AiModelType.CHAT,
                    openai_settings=OpenAiSettings(
                        api_key="fake-key", endpoint="http://localhost:1/", model="fake-model"
                    ),
                )
            )
        )

    def tearDown(self):
        try:
            self.store.maintenance.send(DeleteAiAgentOperation(self.AGENT_IDENTIFIER))
        except Exception:
            pass
        super().tearDown()

    def _create_agent(
        self, send_to_model: bool, parameters_schema: str = None, sample_object: str = "{}"
    ) -> AiAgentConfiguration:
        return AiAgentConfiguration(
            name="support assistant",
            identifier=self.AGENT_IDENTIFIER,
            connection_string_name=self.CONNECTION_STRING_NAME,
            system_prompt="You help users with their own support tickets.",
            sample_object='{"Answer":"The answer to the query"}',
            parameters=[AiAgentParameter("TelegramUsername", "the handle of the sender", send_to_model)],
            queries=[
                AiAgentToolQuery(
                    "MyTickets",
                    "Get the tickets of the current user",
                    "from SupportTickets where TelegramHandle = $TelegramUsername limit 5",
                    parameters_sample_object=sample_object if parameters_schema is None else None,
                    parameters_schema=parameters_schema,
                )
            ],
        )

    def test_the_refusal_reaches_the_client_as_its_own_type(self):
        agent_id = self.store.maintenance.send(
            AddOrUpdateAiAgentOperation(self._create_agent(send_to_model=False))
        ).identifier

        chat = self.store.ai.conversation(
            agent_id,
            "chats/",
            AiConversationCreationOptions().add_parameter("TelegramUsername", {"Handle": "abc123"}),
        )
        chat.set_user_prompt("show my tickets")

        with self.assertRaises(UnsupportedAiQueryParameterException) as context:
            chat.run()

        self.assertIn("$TelegramUsername", context.exception.args[0])

    def test_tool_schema_declaring_the_scoping_parameter_as_optional_is_rejected(self):
        # The parameter is listed in 'properties' with an empty 'required', so the server would otherwise
        # advertise the scoping parameter's name to the model.
        agent = self._create_agent(send_to_model=False, parameters_schema=_OPTIONAL_SCOPING_PARAMETER)

        with self.assertRaises(AiAgentParameterCollisionException) as context:
            self.store.maintenance.send(AddOrUpdateAiAgentOperation(agent))

        self.assertIn("TelegramUsername", context.exception.args[0])

    def test_tool_sample_object_declaring_the_scoping_parameter_is_rejected(self):
        # A schema generated from a sample object lists every property under 'required', so this collision
        # is caught by the older rule, which raises a plain server error.
        agent = self._create_agent(send_to_model=False, sample_object='{"TelegramUsername":"abc"}')

        with self.assertRaises(RavenException):
            self.store.maintenance.send(AddOrUpdateAiAgentOperation(agent))

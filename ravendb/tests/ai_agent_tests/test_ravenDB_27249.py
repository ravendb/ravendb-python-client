import os
import unittest

from ravendb import AiAgentConfiguration, AiAgentToolAction, AiAgentToolQuery
from ravendb.documents.operations.ai import AiConnectionString, AiModelType
from ravendb.documents.operations.ai.agents import DeleteAiAgentOperation
from ravendb.documents.operations.ai.gen_ai_configuration import GenAiConfiguration
from ravendb.documents.operations.ai.gen_ai_transformation import GenAiTransformation
from ravendb.documents.operations.ai.open_ai_settings import OpenAiSettings
from ravendb.documents.operations.connection_string.put_connection_string_operation import PutConnectionStringOperation
from ravendb.tests.test_base import TestBase

_SCHEMA = '{"type":"object","properties":{"id":{"type":"string"}},"required":["id"],"additionalProperties":false}'


class TestRavenDB27249Serialization(unittest.TestCase):
    def test_strict_is_sent_only_when_set(self):
        self.assertNotIn("Strict", AiAgentToolQuery("Q", "d", "from Orders", parameters_schema=_SCHEMA).to_json())
        self.assertNotIn("Strict", AiAgentToolAction("A", "d", parameters_schema=_SCHEMA).to_json())

        self.assertIs(
            True, AiAgentToolQuery("Q", "d", "from Orders", parameters_schema=_SCHEMA, strict=True).to_json()["Strict"]
        )
        self.assertIs(True, AiAgentToolAction("A", "d", parameters_schema=_SCHEMA, strict=True).to_json()["Strict"])

    def test_strict_is_read_back(self):
        self.assertTrue(AiAgentToolQuery.from_json({"Name": "Q", "Strict": True}).strict)
        self.assertTrue(AiAgentToolAction.from_json({"Name": "A", "Strict": True}).strict)

        self.assertFalse(AiAgentToolQuery.from_json({"Name": "Q"}).strict)
        self.assertFalse(AiAgentToolAction.from_json({"Name": "A", "Strict": False}).strict)

    def test_gen_ai_query_keeps_strict(self):
        config = GenAiConfiguration(
            name="genai-strict",
            connection_string_name="genai-cs",
            collection="Docs",
            prompt="Use the query tool to answer.",
            sample_object='{"Answer":"a"}',
            gen_ai_transformation=GenAiTransformation(script="ai.genContext(this);"),
            queries=[
                AiAgentToolQuery(
                    "Q", "run the query", "from Products where id() = $id", parameters_schema=_SCHEMA, strict=True
                )
            ],
        )

        self.assertIs(True, config.to_json()["Queries"][0]["Strict"])
        self.assertTrue(GenAiConfiguration.from_json(config.to_json()).queries[0].strict)


@unittest.skipIf(os.environ.get("RAVENDB_LICENSE") is None, "Insufficient license permissions. Skipping on CI/CD.")
class TestRavenDB27249(TestBase):
    CONNECTION_STRING_NAME = "fake-connection"
    AGENT_IDENTIFIER = "strict-tools"

    def setUp(self):
        super().setUp()
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

    def test_strict_survives_save(self):
        agent = AiAgentConfiguration(
            name="strict tools",
            identifier=self.AGENT_IDENTIFIER,
            connection_string_name=self.CONNECTION_STRING_NAME,
            system_prompt="You answer questions about orders.",
            sample_object='{"Customer":"name","City":"city"}',
            queries=[
                AiAgentToolQuery(
                    "FromSample", "d", "from Orders where id() = $id", parameters_sample_object='{"id": "orders/1"}'
                ),
                AiAgentToolQuery(
                    "QueryStrict", "d", "from Orders where id() = $id", parameters_schema=_SCHEMA, strict=True
                ),
            ],
            actions=[
                AiAgentToolAction("ActionNotStrict", "d", parameters_schema=_SCHEMA),
                AiAgentToolAction("ActionStrict", "d", parameters_schema=_SCHEMA, strict=True),
            ],
        )
        self.store.ai.add_or_update_agent(agent)

        saved = self.store.ai.get_agents(self.AGENT_IDENTIFIER).ai_agents[0]

        self.assertEqual([False, True], [query.strict for query in saved.queries])
        self.assertEqual([False, True], [action.strict for action in saved.actions])


if __name__ == "__main__":
    unittest.main()

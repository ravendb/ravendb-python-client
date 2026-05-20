"""
Integration tests against a live RavenDB 7.2.x server for the AI agent
configuration fields added in 7.2.3:
  * AiAgentConfiguration.sub_agents (+ AiAgentToolSubAgent)
  * AiAgentParameter.policy (AiAgentParameterPolicy)
  * AiAgentParameter.type (AiAgentParameterValueType)

The license guard mirrors the existing AI agent tests in this directory.
"""

import os
import unittest

from ravendb import (
    AiAgentConfiguration,
    AiAgentParameter,
    AiAgentParameterPolicy,
    AiAgentParameterValueType,
    AiAgentToolSubAgent,
)
from ravendb.documents.operations.ai import AiConnectionString, AiModelType
from ravendb.documents.operations.ai.agents import (
    AddOrUpdateAiAgentOperation,
    DeleteAiAgentOperation,
)
from ravendb.documents.operations.ai.open_ai_settings import OpenAiSettings
from ravendb.documents.operations.connection_string.put_connection_string_operation import PutConnectionStringOperation
from ravendb.tests.test_base import TestBase


@unittest.skipIf(os.environ.get("RAVENDB_LICENSE") is None, "Insufficient license permissions. Skipping on CI/CD.")
class TestAiAgentConfigExtensionsIntegration(TestBase):
    CONNECTION_STRING_NAME = "test-ai-agent-cs-ext"

    def setUp(self):
        super().setUp()
        ai_connection_string = AiConnectionString(
            name=self.CONNECTION_STRING_NAME,
            identifier=self.CONNECTION_STRING_NAME,
            model_type=AiModelType.CHAT,
            openai_settings=OpenAiSettings(
                api_key="dummy-api-key",
                endpoint="https://api.openai.com/v1",
                model="gpt-4",
            ),
        )
        self.store.maintenance.send(PutConnectionStringOperation(ai_connection_string))
        self._created_agent_ids = []

    def tearDown(self):
        for agent_id in self._created_agent_ids:
            try:
                self.store.maintenance.send(DeleteAiAgentOperation(agent_id))
            except Exception:
                pass
        super().tearDown()

    # ---- sub_agents ----

    def test_sub_agents_round_trip_through_get_agent(self):
        agent = AiAgentConfiguration(
            name="ParentAgent",
            identifier="test-sub-agent-parent",
            connection_string_name=self.CONNECTION_STRING_NAME,
            system_prompt="Dispatch to sub-agents as needed.",
            sample_object='{"answer": "..."}',
            sub_agents=[
                AiAgentToolSubAgent(identifier="benefits-agent", description="Handles benefit questions"),
                AiAgentToolSubAgent(identifier="attendance-agent", description="Tracks PTO and attendance"),
            ],
        )
        result = self.store.ai.add_or_update_agent(agent)
        self._created_agent_ids.append(result.identifier)

        fetched = self.store.ai.get_agents(result.identifier).ai_agents[0]
        identifiers = sorted(s.identifier for s in fetched.sub_agents)
        self.assertEqual(["attendance-agent", "benefits-agent"], identifiers)

        descriptions = {s.identifier: s.description for s in fetched.sub_agents}
        self.assertEqual("Handles benefit questions", descriptions["benefits-agent"])
        self.assertEqual("Tracks PTO and attendance", descriptions["attendance-agent"])

    # ---- AiAgentParameter.policy ----

    def test_parameter_policy_forbid_model_generation_round_trips(self):
        agent = AiAgentConfiguration(
            name="ParamPolicyAgent",
            identifier="test-param-policy",
            connection_string_name=self.CONNECTION_STRING_NAME,
            system_prompt="Test parameter policy.",
            sample_object='{"answer": "..."}',
            parameters=[
                AiAgentParameter(
                    name="user_id",
                    description="Hidden user id",
                    send_to_model=False,
                    policy=AiAgentParameterPolicy.FORBID_MODEL_GENERATION,
                ),
                AiAgentParameter(name="country", description="The country to filter by."),
            ],
        )
        result = self.store.ai.add_or_update_agent(agent)
        self._created_agent_ids.append(result.identifier)

        fetched = self.store.ai.get_agents(result.identifier).ai_agents[0]
        by_name = {p.name: p for p in fetched.parameters}
        self.assertEqual(AiAgentParameterPolicy.FORBID_MODEL_GENERATION, by_name["user_id"].policy)
        # Default-valued parameter still comes back with the default policy.
        self.assertEqual(AiAgentParameterPolicy.DEFAULT, by_name["country"].policy)

    # ---- AiAgentParameter.type ----

    def test_parameter_value_type_round_trips(self):
        agent = AiAgentConfiguration(
            name="ParamTypeAgent",
            identifier="test-param-type",
            connection_string_name=self.CONNECTION_STRING_NAME,
            system_prompt="Test parameter types.",
            sample_object='{"answer": "..."}',
            parameters=[
                AiAgentParameter(name="email", description="Email", type=AiAgentParameterValueType.STRING),
                AiAgentParameter(name="age", description="Age", type=AiAgentParameterValueType.NUMBER),
                AiAgentParameter(name="tags", description="Tags", type=AiAgentParameterValueType.ARRAY_OF_STRING),
            ],
        )
        result = self.store.ai.add_or_update_agent(agent)
        self._created_agent_ids.append(result.identifier)

        fetched = self.store.ai.get_agents(result.identifier).ai_agents[0]
        by_name = {p.name: p for p in fetched.parameters}
        self.assertEqual(AiAgentParameterValueType.STRING, by_name["email"].type)
        self.assertEqual(AiAgentParameterValueType.NUMBER, by_name["age"].type)
        self.assertEqual(AiAgentParameterValueType.ARRAY_OF_STRING, by_name["tags"].type)


if __name__ == "__main__":
    unittest.main()

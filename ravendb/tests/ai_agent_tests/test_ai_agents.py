"""Integration tests for AI agent CRUD operations."""

import os
import unittest

from ravendb import (
    AiAgentConfiguration,
    AiAgentParameter,
    AiAgentSummarizationByTokens,
    AiAgentChatTrimmingConfiguration,
    AiAgentToolQuery,
    AiAgentToolAction,
)
from ravendb.documents.operations.ai import AiConnectionString, AiModelType
from ravendb.documents.operations.ai.agents import (
    AddOrUpdateAiAgentOperation,
    GetAiAgentOperation,
    DeleteAiAgentOperation,
    AiAgentToolQueryOptions,
)
from ravendb.documents.operations.ai.open_ai_settings import OpenAiSettings
from ravendb.documents.operations.connection_string.put_connection_string_operation import PutConnectionStringOperation
from ravendb.tests.test_base import TestBase


@unittest.skipIf(os.environ.get("RAVENDB_LICENSE") is None, "Insufficient license permissions. Skipping on CI/CD.")
class TestAiAgentCrudOperations(TestBase):
    """Integration tests for AI agent CRUD operations (require server connection)."""

    CONNECTION_STRING_NAME = "test-ai-agent-cs"

    def setUp(self):
        super().setUp()
        ai_connection_string = AiConnectionString(
            name=self.CONNECTION_STRING_NAME,
            identifier="test-ai-agent-cs",
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

    def _create_basic_agent(self, name: str, identifier: str) -> AiAgentConfiguration:
        """Helper to create a minimal valid AiAgentConfiguration."""
        agent = AiAgentConfiguration(
            name=name,
            identifier=identifier,
            connection_string_name=self.CONNECTION_STRING_NAME,
            system_prompt="You are a helpful assistant.",
            sample_object='{"answer": "embed your answer here"}',
        )
        return agent

    def _create_full_agent(self, name: str, identifier: str) -> AiAgentConfiguration:
        """Helper to create a fully configured AiAgentConfiguration."""
        agent = AiAgentConfiguration(
            name=name,
            identifier=identifier,
            connection_string_name=self.CONNECTION_STRING_NAME,
            system_prompt="You are a helpful assistant that queries the database.",
            sample_object='{"answer": "embed your answer here"}',
            parameters=[
                AiAgentParameter("country", "The country to filter by."),
            ],
            queries=[
                AiAgentToolQuery(
                    name="get-orders",
                    description="Retrieve all orders.",
                    query="from Orders",
                    parameters_sample_object="{}",
                ),
            ],
            actions=[
                AiAgentToolAction(
                    name="store-result",
                    description="Store the result in the database.",
                    parameters_sample_object='{"result": "embed result here"}',
                ),
            ],
            chat_trimming=AiAgentChatTrimmingConfiguration(
                tokens_config=AiAgentSummarizationByTokens(
                    max_tokens_before_summarization=32768,
                    max_tokens_after_summarization=1024,
                )
            ),
            max_model_iterations_per_call=3,
        )
        return agent

    # ---- Create ----

    def test_create_agent_returns_identifier(self):
        agent = self._create_basic_agent("TestCreate", "test-create")
        result = self.store.maintenance.send(AddOrUpdateAiAgentOperation(agent))
        self._created_agent_ids.append(result.identifier)

        self.assertIsNotNone(result)
        self.assertEqual("test-create", result.identifier)

    def test_create_agent_via_store_ai(self):
        agent = self._create_basic_agent("TestCreateViaAi", "test-create-via-ai")
        result = self.store.ai.add_or_update_agent(agent)
        self._created_agent_ids.append(result.identifier)

        self.assertIsNotNone(result)
        self.assertEqual("test-create-via-ai", result.identifier)

    def test_create_full_agent(self):
        agent = self._create_full_agent("TestCreateFull", "test-create-full")
        result = self.store.ai.add_or_update_agent(agent)
        self._created_agent_ids.append(result.identifier)

        self.assertIsNotNone(result)
        self.assertEqual("test-create-full", result.identifier)

    # ---- Get ----

    def test_get_all_agents_returns_list(self):
        agent1 = self._create_basic_agent("TestGetAll1", "test-get-all-1")
        agent2 = self._create_basic_agent("TestGetAll2", "test-get-all-2")
        r1 = self.store.ai.add_or_update_agent(agent1)
        r2 = self.store.ai.add_or_update_agent(agent2)
        self._created_agent_ids.extend([r1.identifier, r2.identifier])

        response = self.store.ai.get_agents()

        self.assertIsNotNone(response)
        ids = [a.identifier for a in response.ai_agents]
        self.assertIn("test-get-all-1", ids)
        self.assertIn("test-get-all-2", ids)

    def test_get_agent_by_id(self):
        agent = self._create_basic_agent("TestGetById", "test-get-by-id")
        result = self.store.ai.add_or_update_agent(agent)
        self._created_agent_ids.append(result.identifier)

        response = self.store.ai.get_agents("test-get-by-id")

        self.assertIsNotNone(response)
        self.assertEqual(1, len(response.ai_agents))
        self.assertEqual("test-get-by-id", response.ai_agents[0].identifier)

    def test_get_agent_by_id_returns_correct_config(self):
        agent = self._create_full_agent("TestGetConfig", "test-get-config")
        result = self.store.ai.add_or_update_agent(agent)
        self._created_agent_ids.append(result.identifier)

        response = self.store.ai.get_agents("test-get-config")
        fetched = response.ai_agents[0]

        self.assertEqual("TestGetConfig", fetched.name)
        self.assertEqual(self.CONNECTION_STRING_NAME, fetched.connection_string_name)
        self.assertEqual(1, len(fetched.queries))
        self.assertEqual("get-orders", fetched.queries[0].name)
        self.assertEqual(1, len(fetched.actions))
        self.assertEqual("store-result", fetched.actions[0].name)
        self.assertEqual(1, len(fetched.parameters))
        self.assertEqual("country", fetched.parameters[0].name)
        self.assertEqual(3, fetched.max_model_iterations_per_call)

    def test_get_agent_via_operation(self):
        agent = self._create_basic_agent("TestGetOp", "test-get-op")
        result = self.store.maintenance.send(AddOrUpdateAiAgentOperation(agent))
        self._created_agent_ids.append(result.identifier)

        response = self.store.maintenance.send(GetAiAgentOperation("test-get-op"))

        self.assertIsNotNone(response)
        self.assertEqual(1, len(response.ai_agents))
        self.assertEqual("test-get-op", response.ai_agents[0].identifier)

    # ---- Update ----

    def test_update_agent_system_prompt(self):
        agent = self._create_basic_agent("TestUpdate", "test-update")
        result = self.store.ai.add_or_update_agent(agent)
        self._created_agent_ids.append(result.identifier)

        agent.system_prompt = "Updated system prompt."
        self.store.ai.add_or_update_agent(agent)

        response = self.store.ai.get_agents("test-update")
        self.assertEqual("Updated system prompt.", response.ai_agents[0].system_prompt)

    def test_update_agent_adds_query_tool(self):
        agent = self._create_basic_agent("TestUpdateQuery", "test-update-query")
        result = self.store.ai.add_or_update_agent(agent)
        self._created_agent_ids.append(result.identifier)

        agent.queries.append(
            AiAgentToolQuery(
                name="new-query",
                description="A newly added query tool.",
                query="from Employees",
                parameters_sample_object="{}",
            )
        )
        self.store.ai.add_or_update_agent(agent)

        response = self.store.ai.get_agents("test-update-query")
        query_names = [q.name for q in response.ai_agents[0].queries]
        self.assertIn("new-query", query_names)

    def test_update_agent_max_iterations(self):
        agent = self._create_basic_agent("TestUpdateIter", "test-update-iter")
        result = self.store.ai.add_or_update_agent(agent)
        self._created_agent_ids.append(result.identifier)

        agent.max_model_iterations_per_call = 10
        self.store.ai.add_or_update_agent(agent)

        response = self.store.ai.get_agents("test-update-iter")
        self.assertEqual(10, response.ai_agents[0].max_model_iterations_per_call)

    # ---- Delete ----

    def test_delete_agent(self):
        agent = self._create_basic_agent("TestDelete", "test-delete")
        result = self.store.ai.add_or_update_agent(agent)

        self.store.ai.delete_agent(result.identifier)

        with self.assertRaises(RuntimeError):
            self.store.ai.get_agents("test-delete")

    def test_delete_agent_via_operation(self):
        agent = self._create_basic_agent("TestDeleteOp", "test-delete-op")
        result = self.store.maintenance.send(AddOrUpdateAiAgentOperation(agent))

        self.store.maintenance.send(DeleteAiAgentOperation(result.identifier))

        with self.assertRaises(RuntimeError):
            self.store.maintenance.send(GetAiAgentOperation("test-delete-op"))

    # ---- Full lifecycle ----

    def test_full_lifecycle(self):
        # 1. Create
        agent = self._create_full_agent("TestLifecycle", "test-lifecycle")
        create_result = self.store.ai.add_or_update_agent(agent)
        self.assertEqual("test-lifecycle", create_result.identifier)

        # 2. Get
        response = self.store.ai.get_agents("test-lifecycle")
        self.assertEqual(1, len(response.ai_agents))
        fetched = response.ai_agents[0]
        self.assertEqual("TestLifecycle", fetched.name)

        # 3. Update
        agent.system_prompt = "Updated lifecycle prompt."
        agent.max_model_iterations_per_call = 5
        self.store.ai.add_or_update_agent(agent)

        # 4. Verify update
        updated_response = self.store.ai.get_agents("test-lifecycle")
        updated = updated_response.ai_agents[0]
        self.assertEqual("Updated lifecycle prompt.", updated.system_prompt)
        self.assertEqual(5, updated.max_model_iterations_per_call)

        # 5. Delete
        self.store.ai.delete_agent("test-lifecycle")

        # 6. Verify deletion - server raises error when agent doesn't exist
        with self.assertRaises(RuntimeError):
            self.store.ai.get_agents("test-lifecycle")

    # ---- Query tool options ----

    def test_query_tool_options_persisted(self):
        agent = AiAgentConfiguration(
            identifier="test-query-options",
            name="TestQueryOptions",
            connection_string_name=self.CONNECTION_STRING_NAME,
            system_prompt="You help customers with their orders.",
            sample_object='{"answer": "embed your answer here"}',
            queries=[
                AiAgentToolQuery(
                    name="GetRecentOrders",
                    description="Retrieves recent orders for a customer",
                    query="from Orders where CustomerId = $customerId order by OrderDate desc limit 5",
                    parameters_sample_object='{"customerId": "embed customer id here"}',
                    options=AiAgentToolQueryOptions(
                        add_to_initial_context=True,
                        allow_model_queries=False,
                    ),
                ),
            ],
        )
        self._created_agent_ids.append("test-query-options")

        self.store.ai.add_or_update_agent(agent)

        response = self.store.ai.get_agents("test-query-options")
        self.assertEqual(1, len(response.ai_agents))

        fetched = response.ai_agents[0]
        self.assertEqual(1, len(fetched.queries))

        opts = fetched.queries[0].options
        self.assertIsNotNone(opts)
        self.assertTrue(opts.add_to_initial_context)
        self.assertFalse(opts.allow_model_queries)

    def test_query_tool_options_defaults_when_not_set(self):
        agent = AiAgentConfiguration(
            identifier="test-query-no-options",
            name="TestQueryNoOptions",
            connection_string_name=self.CONNECTION_STRING_NAME,
            system_prompt="Agent without query options.",
            sample_object='{"answer": "embed your answer here"}',
            queries=[
                AiAgentToolQuery(
                    name="GetOrders",
                    description="Retrieves orders",
                    query="from Orders",
                    parameters_sample_object="{}",
                ),
            ],
        )
        self._created_agent_ids.append("test-query-no-options")

        self.store.ai.add_or_update_agent(agent)

        response = self.store.ai.get_agents("test-query-no-options")
        fetched = response.ai_agents[0]
        self.assertEqual(1, len(fetched.queries))
        # No options set — options should be None or have default values
        opts = fetched.queries[0].options
        if opts is not None:
            self.assertIsNone(opts.add_to_initial_context)
            self.assertIsNone(opts.allow_model_queries)

from __future__ import annotations
from typing import TYPE_CHECKING, Optional, Dict, Any, Type

if TYPE_CHECKING:
    from ravendb.documents.store.definition import DocumentStore
    from ravendb.documents.operations.ai.agents import (
        AiAgentConfiguration,
        AiAgentConfigurationResult,
        GetAiAgentsResponse,
    )
    from ravendb.documents.ai.ai_conversation_operations import IAiConversationOperations


class AiOperations:
    """
    AI operations for the document store, providing access to AI agent management and conversation operations.
    """

    def __init__(self, store: DocumentStore):
        self._store = store

    def add_or_update_agent(
        self, configuration: AiAgentConfiguration, schema_type: Type = None
    ) -> AiAgentConfigurationResult:
        """
        Adds or updates an AI agent configuration.

        Args:
            configuration: The AI agent configuration to add or update
            schema_type: Optional type to use for generating sample schema

        Returns:
            Result containing the agent identifier and raft command index
        """
        from ravendb.documents.operations.ai.agents import AddOrUpdateAiAgentOperation

        operation = AddOrUpdateAiAgentOperation(configuration, schema_type)
        return self._store.maintenance.send(operation)

    def delete_agent(self, identifier: str) -> AiAgentConfigurationResult:
        """
        Deletes an AI agent configuration.

        Args:
            identifier: The identifier of the agent to delete

        Returns:
            Result containing the raft command index
        """
        from ravendb.documents.operations.ai.agents import DeleteAiAgentOperation

        operation = DeleteAiAgentOperation(identifier)
        return self._store.maintenance.send(operation)

    def get_agents(self, agent_id: str = None) -> GetAiAgentsResponse:
        """
        Gets AI agent configurations.

        Args:
            agent_id: Optional specific agent ID to retrieve. If None, returns all agents.

        Returns:
            Response containing the list of AI agent configurations
        """
        from ravendb.documents.operations.ai.agents import GetAiAgentOperation

        operation = GetAiAgentOperation(agent_id)
        return self._store.maintenance.send(operation)

    def conversation(self, agent_id: str, parameters: Dict[str, Any] = None) -> IAiConversationOperations:
        """
        Creates a new conversation with the specified AI agent.

        Args:
            agent_id: The identifier of the AI agent to start a conversation with
            parameters: Optional parameters to pass to the agent

        Returns:
            Conversation operations interface for managing the conversation
        """
        from ravendb.documents.ai.ai_conversation import AiConversation

        return AiConversation(self._store, agent_id, parameters)

    def conversation_with_id(self, conversation_id: str, change_vector: str = None) -> IAiConversationOperations:
        """
        Continues an existing conversation by its ID.

        Args:
            conversation_id: The ID of the existing conversation
            change_vector: Optional change vector for optimistic concurrency

        Returns:
            Conversation operations interface for managing the conversation
        """
        from ravendb.documents.ai.ai_conversation import AiConversation

        return AiConversation.with_conversation_id(self._store, conversation_id, change_vector)

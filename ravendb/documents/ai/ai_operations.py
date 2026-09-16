from __future__ import annotations
from typing import TYPE_CHECKING, Dict, Any, Optional, Type, Union

import warnings

from ravendb.documents.ai.ai_conversation import AiConversation

if TYPE_CHECKING:
    from ravendb.documents.store.definition import DocumentStore
    from ravendb import AiConversationCreationOptions
    from ravendb.documents.operations.ai.agents import (
        AiAgentConfiguration,
        AiAgentConfigurationResult,
        AiConversationMessagesResult,
        GetAiAgentsResponse,
        GetConversationMessagesOptions,
    )


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

    def get_conversation_messages(
        self, conversation_id_or_parameters: Union[str, "GetConversationMessagesOptions"]
    ) -> "AiConversationMessagesResult":
        """
        Reads messages from an AI conversation. Returns the most recent messages by default.

        Args:
            conversation_id_or_parameters: The conversation document ID, or a
                GetConversationMessagesOptions for full control over paging
                (before/after timestamps), page size, and detail level

        Returns:
            The conversation's messages, cumulative usage, and paging state
        """
        from ravendb.documents.operations.ai.agents import GetConversationMessagesOperation

        operation = GetConversationMessagesOperation(conversation_id_or_parameters)
        return self._store.maintenance.send(operation)

    def conversation(
        self,
        agent_id: str,
        conversation_id: str,
        creation_options: "AiConversationCreationOptions" = None,
        change_vector: str = None,
        debug: Optional[bool] = None,
        cancel_pending_action_tools: bool = False,
    ) -> AiConversation:
        """
        Creates a new conversation with the specified AI agent.

        Args:
            agent_id: The identifier of the AI agent to start a conversation with
            conversation_id: The unique identifier for the conversation. You can also use e.g. chats/ for automatic id.
            creation_options: Optional creation options for the conversation
            change_vector: Optional change vector for concurrency control
            debug: Optional flag enabling server-side conversation debugging
            cancel_pending_action_tools: Drop the tool calls the conversation is still waiting on
                instead of answering them, on the next run. Cleared once that run succeeds.

        Returns:
            Conversation operations interface for managing the conversation
        """

        return AiConversation(
            self._store,
            agent_id,
            creation_options,
            conversation_id,
            change_vector,
            debug,
            cancel_pending_action_tools,
        )

    def conversation_with_id(self, conversation_id: str, change_vector: str = None) -> AiConversation:
        """
        Continues an existing conversation by its ID.

        Args:
            conversation_id: The ID of the existing conversation
            change_vector: Optional change vector for optimistic concurrency

        Returns:
            Conversation operations interface for managing the conversation
        """
        warnings.warn(
            "AiOperations.conversation_with_id(...) is deprecated; use AiOperations.conversation(agent_id, conversation_id=..., change_vector=...) instead.",
            DeprecationWarning,
            stacklevel=2,
        )

        from ravendb.documents.ai.ai_conversation import AiConversation

        return AiConversation.with_conversation_id(self._store, conversation_id, change_vector)

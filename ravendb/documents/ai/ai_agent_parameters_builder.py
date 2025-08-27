from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Dict, Any, TypeVar, Generic, TYPE_CHECKING

if TYPE_CHECKING:
    from ravendb.documents.ai.ai_conversation_operations import IAiConversationOperations

TResponse = TypeVar("TResponse")


class IAiAgentParametersBuilder(ABC, Generic[TResponse]):
    """
    Interface for building parameters for AI agent conversations.
    """

    @abstractmethod
    def with_parameter(self, name: str, value: Any) -> IAiAgentParametersBuilder[TResponse]:
        """
        Adds a parameter to the conversation.

        Args:
            name: The parameter name
            value: The parameter value

        Returns:
            The builder instance for method chaining
        """
        pass

    @abstractmethod
    def build(self) -> IAiConversationOperations[TResponse]:
        """
        Builds and returns the conversation operations instance.

        Returns:
            The conversation operations interface
        """
        pass


class AiAgentParametersBuilder(IAiAgentParametersBuilder[TResponse]):
    """
    Builder for constructing AI agent conversation parameters.
    """

    def __init__(self, conversation_factory):
        """
        Initializes the parameters builder.

        Args:
            conversation_factory: A callable that creates the conversation with the built parameters
        """
        self._parameters: Dict[str, Any] = {}
        self._conversation_factory = conversation_factory

    def with_parameter(self, name: str, value: Any) -> IAiAgentParametersBuilder[TResponse]:
        """
        Adds a parameter to the conversation.

        Args:
            name: The parameter name
            value: The parameter value

        Returns:
            The builder instance for method chaining
        """
        self._parameters[name] = value
        return self

    def build(self) -> IAiConversationOperations[TResponse]:
        """
        Builds and returns the conversation operations instance.

        Returns:
            The conversation operations interface
        """
        return self._conversation_factory(self._parameters)

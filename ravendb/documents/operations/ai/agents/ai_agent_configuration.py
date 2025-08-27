from __future__ import annotations
from typing import List, Set, Optional, Dict, Any


class AiAgentToolQuery:
    """
    Represents a query tool that can be invoked by an AI agent.
    The tool includes a name, description, query string, and parameter schema or sample object.
    When invoked by the AI model, the query is expected to be executed by the server (database),
    and its results provided back to the model.
    """

    def __init__(self, name: str = None, description: str = None, query: str = None):
        self.name = name
        self.description = description
        self.query = query
        self.parameters_sample_object: Optional[str] = None
        self.parameters_schema: Optional[str] = None

    def to_json(self) -> Dict[str, Any]:
        return {
            "Name": self.name,
            "Description": self.description,
            "Query": self.query,
            "ParametersSampleObject": self.parameters_sample_object,
            "ParametersSchema": self.parameters_schema,
        }

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> AiAgentToolQuery:
        instance = cls()
        instance.name = json_dict.get("name") or json_dict.get("Name")
        instance.description = json_dict.get("description") or json_dict.get("Description")
        instance.query = json_dict.get("query") or json_dict.get("Query")
        instance.parameters_sample_object = json_dict.get("parametersSampleObject") or json_dict.get(
            "ParametersSampleObject"
        )
        instance.parameters_schema = json_dict.get("parametersSchema") or json_dict.get("ParametersSchema")
        return instance


class AiAgentToolAction:
    """
    Represents a tool action that can be invoked by an AI agent.
    Includes metadata such as name, description, and optional parameters schema or sample.
    Tool actions represent external functions whose results are provided by the user
    """

    def __init__(self, name: str = None, description: str = None):
        self.name = name
        self.description = description
        self.parameters_sample_object: Optional[str] = None
        self.parameters_schema: Optional[str] = None

    def to_json(self) -> Dict[str, Any]:
        return {
            "Name": self.name,
            "Description": self.description,
            "ParametersSampleObject": self.parameters_sample_object,
            "ParametersSchema": self.parameters_schema,
        }

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> AiAgentToolAction:
        instance = cls()
        instance.name = json_dict.get("name") or json_dict.get("Name")
        instance.description = json_dict.get("description") or json_dict.get("Description")
        instance.parameters_sample_object = json_dict.get("parametersSampleObject") or json_dict.get(
            "ParametersSampleObject"
        )
        instance.parameters_schema = json_dict.get("parametersSchema") or json_dict.get("ParametersSchema")
        return instance


class AiAgentPersistenceConfiguration:
    """
    Configuration for persisting chat history in RavenDB.
    Defines where chat sessions should be stored and optionally how long they should be retained (expiration).
    """

    def __init__(self, conversation_id_prefix: str = None, expires: int = None):
        self.conversation_id_prefix = conversation_id_prefix
        self.conversation_expiration_in_sec: Optional[int] = expires

    def to_json(self) -> Dict[str, Any]:
        return {
            "ConversationIdPrefix": self.conversation_id_prefix,
            "ConversationExpirationInSec": self.conversation_expiration_in_sec,
        }

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> AiAgentPersistenceConfiguration:
        instance = cls()
        instance.conversation_id_prefix = json_dict.get("conversationIdPrefix") or json_dict.get("ConversationIdPrefix")
        instance.conversation_expiration_in_sec = json_dict.get("conversationExpirationInSec") or json_dict.get(
            "ConversationExpirationInSec"
        )
        return instance


class AiAgentSummarizationByTokens:
    """
    Configuration settings for AI agent conversation summarization.
    """

    DEFAULT_MAX_TOKENS_BEFORE_SUMMARIZATION = 32 * 1024

    def __init__(self):
        self.summarization_task_beginning_prompt: Optional[str] = None
        self.summarization_task_end_prompt: Optional[str] = None
        self.result_prefix: Optional[str] = None
        self.max_tokens_before_summarization: int = self.DEFAULT_MAX_TOKENS_BEFORE_SUMMARIZATION
        self.max_tokens_after_summarization: int = 1024

    def to_json(self) -> Dict[str, Any]:
        return {
            "SummarizationTaskBeginningPrompt": self.summarization_task_beginning_prompt,
            "SummarizationTaskEndPrompt": self.summarization_task_end_prompt,
            "ResultPrefix": self.result_prefix,
            "MaxTokensBeforeSummarization": self.max_tokens_before_summarization,
            "MaxTokensAfterSummarization": self.max_tokens_after_summarization,
        }

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> AiAgentSummarizationByTokens:
        instance = cls()
        instance.summarization_task_beginning_prompt = json_dict.get("SummarizationTaskBeginningPrompt")
        instance.summarization_task_end_prompt = json_dict.get("SummarizationTaskEndPrompt")
        instance.result_prefix = json_dict.get("ResultPrefix")
        instance.max_tokens_before_summarization = json_dict.get(
            "MaxTokensBeforeSummarization", cls.DEFAULT_MAX_TOKENS_BEFORE_SUMMARIZATION
        )
        instance.max_tokens_after_summarization = json_dict.get("MaxTokensAfterSummarization", 1024)
        return instance


class AiAgentTruncateChat:
    """
    Configuration for truncating the AI chat history based on message count.
    """

    DEFAULT_MESSAGES_LENGTH_BEFORE_TRUNCATE = 500

    def __init__(self):
        self.messages_length_before_truncate: int = self.DEFAULT_MESSAGES_LENGTH_BEFORE_TRUNCATE
        self.messages_length_after_truncate: int = self.DEFAULT_MESSAGES_LENGTH_BEFORE_TRUNCATE // 2

    def to_json(self) -> Dict[str, Any]:
        return {
            "MessagesLengthBeforeTruncate": self.messages_length_before_truncate,
            "MessagesLengthAfterTruncate": self.messages_length_after_truncate,
        }

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> AiAgentTruncateChat:
        instance = cls()
        instance.messages_length_before_truncate = json_dict.get(
            "MessagesLengthBeforeTruncate", cls.DEFAULT_MESSAGES_LENGTH_BEFORE_TRUNCATE
        )
        instance.messages_length_after_truncate = json_dict.get(
            "MessagesLengthAfterTruncate", cls.DEFAULT_MESSAGES_LENGTH_BEFORE_TRUNCATE // 2
        )
        return instance


class AiAgentHistoryConfiguration:
    """
    Defines the configuration for retention and expiration of AI agent chat history documents.
    """

    def __init__(self):
        self.history_expiration_in_sec: Optional[int] = None

    def to_json(self) -> Dict[str, Any]:
        return {
            "HistoryExpirationInSec": self.history_expiration_in_sec,
        }

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> AiAgentHistoryConfiguration:
        instance = cls()
        instance.history_expiration_in_sec = json_dict.get("HistoryExpirationInSec")
        return instance


class AiAgentChatTrimmingConfiguration:
    """
    Defines configuration options for reducing the size of the AI agent's chat history.
    """

    def __init__(
        self,
        tokens_config: AiAgentSummarizationByTokens = None,
        truncate_config: AiAgentTruncateChat = None,
        history_config: AiAgentHistoryConfiguration = None,
    ):
        self.tokens: Optional[AiAgentSummarizationByTokens] = tokens_config
        self.truncate: Optional[AiAgentTruncateChat] = truncate_config
        self.history: Optional[AiAgentHistoryConfiguration] = history_config

    def to_json(self) -> Dict[str, Any]:
        return {
            "Tokens": self.tokens.to_json() if self.tokens else None,
            "Truncate": self.truncate.to_json() if self.truncate else None,
            "History": self.history.to_json() if self.history else None,
        }

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> AiAgentChatTrimmingConfiguration:
        instance = cls()
        if json_dict.get("Tokens"):
            instance.tokens = AiAgentSummarizationByTokens.from_json(json_dict["Tokens"])
        if json_dict.get("Truncate"):
            instance.truncate = AiAgentTruncateChat.from_json(json_dict["Truncate"])
        if json_dict.get("History"):
            instance.history = AiAgentHistoryConfiguration.from_json(json_dict["History"])
        return instance


class AiAgentConfiguration:
    """
    Defines the configuration for an AI agent in RavenDB, including the system prompt,
    tools (queries/actions), output schema, persistence settings, and connection string.
    """

    def __init__(self, name: str = None, connection_string_name: str = None, system_prompt: str = None):
        self.identifier: Optional[str] = None
        self.name = name
        self.connection_string_name = connection_string_name
        self.system_prompt = system_prompt
        self.sample_object: Optional[str] = None
        self.output_schema: Optional[str] = None
        self.queries: List[AiAgentToolQuery] = []
        self.actions: List[AiAgentToolAction] = []
        self.persistence: Optional[AiAgentPersistenceConfiguration] = None
        self.parameters: Set[str] = set()
        self.chat_trimming: Optional[AiAgentChatTrimmingConfiguration] = None
        self.max_model_iterations_per_call: Optional[int] = None

    def to_json(self) -> Dict[str, Any]:
        # Convert parameters set to list of parameter objects using list comprehension
        parameters_list = [{"Name": param_name, "Description": None} for param_name in self.parameters]

        return {
            "Identifier": self.identifier,
            "Name": self.name,
            "ConnectionStringName": self.connection_string_name,
            "SystemPrompt": self.system_prompt,
            "SampleObject": self.sample_object,
            "OutputSchema": self.output_schema,
            "Queries": [q.to_json() for q in self.queries],
            "Actions": [a.to_json() for a in self.actions],
            "Persistence": self.persistence.to_json() if self.persistence else None,
            "Parameters": parameters_list,
            "ChatTrimming": self.chat_trimming.to_json() if self.chat_trimming else None,
            "MaxModelIterationsPerCall": self.max_model_iterations_per_call,
        }

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> AiAgentConfiguration:
        instance = cls()
        # Handle both camelCase and PascalCase for compatibility
        instance.identifier = json_dict.get("identifier") or json_dict.get("Identifier")
        instance.name = json_dict.get("name") or json_dict.get("Name")
        instance.connection_string_name = json_dict.get("connectionStringName") or json_dict.get("ConnectionStringName")
        instance.system_prompt = json_dict.get("systemPrompt") or json_dict.get("SystemPrompt")
        instance.sample_object = json_dict.get("sampleObject") or json_dict.get("SampleObject")
        instance.output_schema = json_dict.get("outputSchema") or json_dict.get("OutputSchema")

        queries_data = json_dict.get("queries") or json_dict.get("Queries")
        if queries_data:
            instance.queries = [AiAgentToolQuery.from_json(q) for q in queries_data]

        actions_data = json_dict.get("actions") or json_dict.get("Actions")
        if actions_data:
            instance.actions = [AiAgentToolAction.from_json(a) for a in actions_data]

        persistence_data = json_dict.get("persistence") or json_dict.get("Persistence")
        if persistence_data:
            instance.persistence = AiAgentPersistenceConfiguration.from_json(persistence_data)

        params_data = json_dict.get("parameters") or json_dict.get("Parameters")
        if params_data:
            # Handle both string list and object list formats
            if params_data and isinstance(params_data[0], dict):
                # New format: list of objects with name property
                instance.parameters = set(param.get("name") or param.get("Name") for param in params_data)
            else:
                # Old format: list of strings
                instance.parameters = set(params_data)

        trimming_data = json_dict.get("chatTrimming") or json_dict.get("ChatTrimming")
        if trimming_data:
            instance.chat_trimming = AiAgentChatTrimmingConfiguration.from_json(trimming_data)

        instance.max_model_iterations_per_call = json_dict.get("maxModelIterationsPerCall") or json_dict.get(
            "MaxModelIterationsPerCall"
        )
        return instance

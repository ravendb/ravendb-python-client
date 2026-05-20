from __future__ import annotations
import enum
from typing import List, Optional, Dict, Any, Union


class AiAgentParameterPolicy(enum.IntFlag):
    # FORBID_MODEL_GENERATION blocks a parent agent from generating values for
    # this parameter when invoking a sub-agent that declares it.
    DEFAULT = 0
    FORBID_MODEL_GENERATION = 1


class AiAgentParameterValueType(enum.Enum):
    DEFAULT = "Default"
    STRING = "String"
    NUMBER = "Number"
    BOOLEAN = "Boolean"
    ARRAY_OF_STRING = "ArrayOfString"
    ARRAY_OF_NUMBER = "ArrayOfNumber"
    ARRAY_OF_BOOLEAN = "ArrayOfBoolean"
    NULL = "Null"

    def __str__(self) -> str:
        return self.value


class AiAgentParameter:
    def __init__(
        self,
        name: str = None,
        description: str = None,
        send_to_model: bool = None,
        policy: AiAgentParameterPolicy = AiAgentParameterPolicy.DEFAULT,
        type: AiAgentParameterValueType = AiAgentParameterValueType.DEFAULT,
    ):
        self.name = name
        self.description: Optional[str] = description
        self.send_to_model: Optional[bool] = send_to_model
        self.policy: AiAgentParameterPolicy = policy
        self.type: AiAgentParameterValueType = type

    def to_json(self) -> Dict[str, Any]:
        return {
            "Name": self.name,
            "Description": self.description,
            "SendToModel": self.send_to_model,
            "Policy": int(self.policy),
            "Type": self.type.value,
        }

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> AiAgentParameter:
        # Server emits Policy as either int (1) or PascalCase ("ForbidModelGeneration").
        policy_raw = json_dict.get("policy") if "policy" in json_dict else json_dict.get("Policy")
        if policy_raw is None or policy_raw == 0 or policy_raw == "":
            policy = AiAgentParameterPolicy.DEFAULT
        elif isinstance(policy_raw, str):
            snake = "".join("_" + c if i > 0 and c.isupper() else c for i, c in enumerate(policy_raw)).upper()
            policy = AiAgentParameterPolicy[snake]
        else:
            policy = AiAgentParameterPolicy(policy_raw)

        type_raw = json_dict.get("type") if "type" in json_dict else json_dict.get("Type")
        type_ = AiAgentParameterValueType(type_raw) if type_raw else AiAgentParameterValueType.DEFAULT

        return cls(
            name=json_dict.get("name") or json_dict.get("Name"),
            description=json_dict.get("description") or json_dict.get("Description"),
            send_to_model=json_dict.get("sendToModel") if "sendToModel" in json_dict else json_dict.get("SendToModel"),
            policy=policy,
            type=type_,
        )


class AiAgentToolQuery:
    # Database-side RQL the model can call. Results are sent back to the model.
    def __init__(
        self,
        name: str = None,
        description: str = None,
        query: str = None,
        parameters_sample_object: str = None,
        parameters_schema: str = None,
        options: AiAgentToolQueryOptions = None,
    ):
        self.name = name
        self.description = description
        self.query = query
        self.parameters_sample_object: Optional[str] = parameters_sample_object
        self.parameters_schema: Optional[str] = parameters_schema
        self.options = options

    def to_json(self) -> Dict[str, Any]:
        json_dict = {
            "Name": self.name,
            "Description": self.description,
            "Query": self.query,
            "ParametersSampleObject": self.parameters_sample_object,
            "ParametersSchema": self.parameters_schema,
        }
        if self.options:
            json_dict["Options"] = self.options.to_json()

        return json_dict

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
        if options := json_dict.get("Options"):
            instance.options = AiAgentToolQueryOptions.from_json(options)
        return instance


class AiAgentToolAction:
    # External function the model can call. Its result is supplied by the user
    # (vs AiAgentToolQuery whose result comes from the database).
    def __init__(
        self,
        name: str = None,
        description: str = None,
        parameters_sample_object: str = None,
        parameters_schema: str = None,
    ):
        self.name = name
        self.description = description
        self.parameters_sample_object: Optional[str] = parameters_sample_object
        self.parameters_schema: Optional[str] = parameters_schema

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
    DEFAULT_MAX_TOKENS_BEFORE_SUMMARIZATION = 32 * 1024

    def __init__(
        self,
        summarization_task_beginning_prompt: str = None,
        summarization_task_end_prompt: str = None,
        result_prefix: str = None,
        max_tokens_before_summarization: int = None,
        max_tokens_after_summarization: int = None,
    ):
        self.summarization_task_beginning_prompt: Optional[str] = summarization_task_beginning_prompt
        self.summarization_task_end_prompt: Optional[str] = summarization_task_end_prompt
        self.result_prefix: Optional[str] = result_prefix
        self.max_tokens_before_summarization: int = (
            max_tokens_before_summarization or self.DEFAULT_MAX_TOKENS_BEFORE_SUMMARIZATION
        )
        self.max_tokens_after_summarization: int = max_tokens_after_summarization or 1024

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
    DEFAULT_MESSAGES_LENGTH_BEFORE_TRUNCATE = 500

    def __init__(self, messages_length_before_truncate: int = None, messages_length_after_truncate: int = None):
        self.messages_length_before_truncate: int = (
            messages_length_before_truncate or self.DEFAULT_MESSAGES_LENGTH_BEFORE_TRUNCATE
        )
        self.messages_length_after_truncate: int = (
            messages_length_after_truncate or self.DEFAULT_MESSAGES_LENGTH_BEFORE_TRUNCATE // 2
        )

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
    def __init__(self, history_expiration_in_sec: int = None):
        self.history_expiration_in_sec: Optional[int] = history_expiration_in_sec

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
    def __init__(
        self,
        name: str = None,
        connection_string_name: str = None,
        system_prompt: str = None,
        identifier: str = None,
        sample_object: str = None,
        output_schema: str = None,
        queries: List[AiAgentToolQuery] = None,
        actions: List[AiAgentToolAction] = None,
        persistence: AiAgentPersistenceConfiguration = None,
        parameters: List[Union[str, AiAgentParameter]] = None,
        chat_trimming: AiAgentChatTrimmingConfiguration = None,
        max_model_iterations_per_call: int = None,
        disabled: bool = False,
    ):
        self.name = name
        self.connection_string_name = connection_string_name
        self.system_prompt = system_prompt
        self.identifier: Optional[str] = identifier
        self.sample_object: Optional[str] = sample_object
        self.output_schema: Optional[str] = output_schema
        self.queries: List[AiAgentToolQuery] = queries or []
        self.actions: List[AiAgentToolAction] = actions or []
        self.persistence: Optional[AiAgentPersistenceConfiguration] = persistence
        self.parameters: List[AiAgentParameter] = self._normalize_parameters(parameters)
        self.chat_trimming: Optional[AiAgentChatTrimmingConfiguration] = chat_trimming
        self.max_model_iterations_per_call: Optional[int] = max_model_iterations_per_call
        self.disabled: bool = disabled

    @staticmethod
    def _normalize_parameters(parameters: List[Union[str, AiAgentParameter]]) -> List[AiAgentParameter]:
        """Convert a list of strings or AiAgentParameter objects to a list of AiAgentParameter objects."""
        if not parameters:
            return []
        result = []
        for param in parameters:
            if isinstance(param, str):
                result.append(AiAgentParameter(name=param))
            else:
                result.append(param)
        return result

    def to_json(self) -> Dict[str, Any]:
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
            "Parameters": [p.to_json() for p in self.parameters],
            "ChatTrimming": self.chat_trimming.to_json() if self.chat_trimming else None,
            "MaxModelIterationsPerCall": self.max_model_iterations_per_call,
            "Disabled": self.disabled,
        }

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> AiAgentConfiguration:
        instance = cls()
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
            instance.parameters = [AiAgentParameter.from_json(param) for param in params_data]

        trimming_data = json_dict.get("chatTrimming") or json_dict.get("ChatTrimming")
        if trimming_data:
            instance.chat_trimming = AiAgentChatTrimmingConfiguration.from_json(trimming_data)

        instance.max_model_iterations_per_call = json_dict.get("maxModelIterationsPerCall") or json_dict.get(
            "MaxModelIterationsPerCall"
        )
        instance.disabled = json_dict.get("disabled", False) or json_dict.get("Disabled", False)
        return instance


class AiAgentToolQueryOptions:
    def __init__(self, allow_model_queries: bool = None, add_to_initial_context: bool = None):
        self.allow_model_queries = allow_model_queries
        self.add_to_initial_context = add_to_initial_context

    def to_json(self) -> Dict[str, Any]:
        return {
            "AllowModelQueries": self.allow_model_queries,
            "AddToInitialContext": self.add_to_initial_context,
        }

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> AiAgentToolQueryOptions:
        return cls(
            allow_model_queries=json_dict["AllowModelQueries"],
            add_to_initial_context=json_dict["AddToInitialContext"],
        )

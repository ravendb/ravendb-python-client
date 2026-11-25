import enum
from abc import ABC
from typing import Any, Dict, Optional

from ravendb.documents.operations.ai.open_ai_base_settings import OpenAiBaseSettings


class OpenAiChatApiType(enum.Enum):
    # The /responses API.
    RESPONSES = "Responses"
    # The /chat/completions API.
    CHAT_COMPLETIONS = "ChatCompletions"


class OpenAiChatApiSettings(OpenAiBaseSettings, ABC):
    # Base for the connectors whose chat requests can go to either OpenAI chat API: OpenAI, Azure OpenAI and Ollama.
    def __init__(
        self,
        api_key: str = None,
        endpoint: str = None,
        model: str = None,
        dimensions: int = None,
        temperature: float = None,
        embeddings_max_concurrent_batches: int = None,
        enable_prompt_cache: bool = None,
        reasoning_effort: str = None,
        chat_api: OpenAiChatApiType = None,
    ):
        super().__init__(
            api_key, endpoint, model, dimensions, temperature, embeddings_max_concurrent_batches, enable_prompt_cache
        )
        # The reasoning_effort sent to the provider, controlling the reasoning depth of supported models
        # (such as the GPT-5 family). Typical values are "none", "minimal", "low", "medium", "high", "xhigh"
        # and "max"; model families accept different subsets. Sent as supplied apart from trimming and not
        # validated, so new provider values can be used directly. OpenAI accepts lowercase values only.
        # When not set the field is omitted and the model applies its own default.
        self.reasoning_effort = reasoning_effort
        # The API that chat requests are sent to. When not set, the connector's default is used: OpenAI uses
        # Responses for an empty endpoint or api.openai.com and Chat Completions for any other endpoint,
        # Azure OpenAI uses Responses, Ollama uses Chat Completions.
        self.chat_api: Optional[OpenAiChatApiType] = chat_api

    def _add_chat_api_fields(self, json_dict: Dict[str, Any]) -> Dict[str, Any]:
        if self.chat_api is not None:
            json_dict["ChatApi"] = self.chat_api.value
        if self.reasoning_effort is not None and self.reasoning_effort.strip():
            json_dict["ReasoningEffort"] = self.reasoning_effort
        return json_dict

    @staticmethod
    def _chat_api_from_json(json_dict: Dict[str, Any]) -> Optional[OpenAiChatApiType]:
        chat_api = json_dict.get("ChatApi")
        return OpenAiChatApiType(chat_api) if chat_api else None

    @staticmethod
    def _reasoning_effort_from_json(json_dict: Dict[str, Any]) -> Optional[str]:
        # A numeric value persisted by earlier enum-based versions reads as not configured
        reasoning_effort = json_dict.get("ReasoningEffort")
        return reasoning_effort if isinstance(reasoning_effort, str) else None

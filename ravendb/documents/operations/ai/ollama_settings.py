from typing import Dict, Any

from ravendb.documents.operations.ai.open_ai_chat_api_settings import OpenAiChatApiSettings, OpenAiChatApiType


class OllamaSettings(OpenAiChatApiSettings):
    def __init__(
        self,
        uri: str = None,
        model: str = None,
        think: bool = None,
        temperature: float = None,
        embeddings_max_concurrent_batches: int = None,
        api_key: str = None,
        reasoning_effort: str = None,
        chat_api: OpenAiChatApiType = None,
        enable_prompt_cache: bool = None,
    ):
        # api_key is optional: local Ollama deployments usually have none. When set, it is sent as a Bearer token
        # on chat requests (for an Ollama behind an authenticating proxy); embeddings requests do not send it.
        super().__init__(
            api_key=api_key,
            endpoint=uri,
            model=model,
            temperature=temperature,
            embeddings_max_concurrent_batches=embeddings_max_concurrent_batches,
            enable_prompt_cache=enable_prompt_cache,
            reasoning_effort=reasoning_effort,
            chat_api=chat_api,
        )
        self.think = think

    @property
    def uri(self) -> str:
        # The URI of the Ollama API, the same value as endpoint; stored as "Uri"
        return self.endpoint

    @uri.setter
    def uri(self, value: str):
        self.endpoint = value

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> "OllamaSettings":
        return cls(
            uri=json_dict["Uri"] if "Uri" in json_dict else json_dict.get("Endpoint"),
            model=json_dict["Model"] if "Model" in json_dict else None,
            think=json_dict["Think"] if "Think" in json_dict else None,
            temperature=json_dict["Temperature"] if "Temperature" in json_dict else None,
            embeddings_max_concurrent_batches=(
                json_dict["EmbeddingsMaxConcurrentBatches"] if "EmbeddingsMaxConcurrentBatches" in json_dict else None
            ),
            api_key=json_dict.get("ApiKey"),
            reasoning_effort=cls._reasoning_effort_from_json(json_dict),
            chat_api=cls._chat_api_from_json(json_dict),
            enable_prompt_cache=json_dict.get("EnablePromptCache"),
        )

    def to_json(self) -> Dict[str, Any]:
        json_dict = {
            "Uri": self.uri,
            "Model": self.model,
            "Think": self.think,
            "Temperature": self.temperature,
            "EmbeddingsMaxConcurrentBatches": self.embeddings_max_concurrent_batches,
        }
        if self.api_key is not None and self.api_key.strip():
            json_dict["ApiKey"] = self.api_key
        if self.enable_prompt_cache is not None:
            json_dict["EnablePromptCache"] = self.enable_prompt_cache
        return self._add_chat_api_fields(json_dict)
